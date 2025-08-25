import asyncio
import os
import uuid
import re
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import gradio as gr
from dotenv import load_dotenv
from google.adk.agents.run_config import RunConfig, StreamingMode
from google.adk.artifacts import GcsArtifactService
from google.cloud import storage
from google.genai import types as genai_types

from dbt_query_tool_agent.agent import root_agent
from dbt_query_tool_agent.services.runner import create_runner
from dbt_query_tool_agent.services.session import create_session
from dbt_query_tool_agent.setup.initialization import init_vertexai

# Build a path to the .env file in the project root directory.
# This makes the script independent of the current working directory.
dotenv_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# --- Configuration ---
# Prioritize standard `GOOGLE_CLOUD_*` variables, but fall back to legacy `GCP_*` names
# for backward compatibility with older .env files.
GCP_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
GCP_LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION") or os.environ.get("GCP_LOCATION")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "shruti_test3")  # Default bucket
APP_NAME = "dbt-query-generator-gradio"

# --- Initialization ---
# This block must run before any other project modules are imported.
gcs_service = None
storage_client = None
try:
    if not GCP_PROJECT_ID or not GCP_LOCATION:
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION environment variables must be set. "
            "Please check your .env file."
        )

    init_vertexai(GCP_PROJECT_ID, GCP_LOCATION)
    print(f"Vertex AI initialized for project '{GCP_PROJECT_ID}' in '{GCP_LOCATION}'.")

    # Initialize the storage client for file uploads
    storage_client = storage.Client(project=GCP_PROJECT_ID)

    # GcsArtifactService expects just the bucket name, not the 'gs://' prefix.
    bucket_name_for_service = GCS_BUCKET_NAME.replace("gs://", "")
    gcs_service = GcsArtifactService(bucket_name=bucket_name_for_service)
    print(f"GcsArtifactService initialized for bucket: {bucket_name_for_service}")

except (ValueError, Exception) as e:
    print(f"ERROR during initialization: {e}")
    # Allow Gradio to launch but show an error in the chat.

# --- Robust StreamingMode detection ---
# Find a valid streaming mode attribute, trying common names to support different ADK versions.
if hasattr(StreamingMode, 'STREAMING'):
    STREAMING_ENUM_VALUE = StreamingMode.STREAMING
elif hasattr(StreamingMode, 'FINAL_RESPONSE_STREAM'):
    STREAMING_ENUM_VALUE = StreamingMode.FINAL_RESPONSE_STREAM
else:
    # As a last resort, fallback to non-streaming mode if no known streaming attribute is found.
    print("WARNING: Could not find a known streaming mode. "
          "Falling back to non-streaming mode. Agent responses will not be streamed.")
    STREAMING_ENUM_VALUE = StreamingMode.NONE

async def handle_file_upload(file, session_state: Any, progress=gr.Progress()):
    """
    Handles uploading a file to GCS and updating the session state.
    """
    if not file:
        return None, session_state

    if not storage_client or not GCS_BUCKET_NAME:
        gr.Warning("GCS is not configured on the server. Cannot upload file.")
        return None, session_state

    # Ensure session_id exists for organizing uploads
    if "session_id" not in session_state:
        session_state["session_id"] = f"session_{uuid.uuid4().hex[:8]}"

    session_id = session_state["session_id"]
    bucket_name = GCS_BUCKET_NAME.replace("gs://", "")
    bucket = storage_client.bucket(bucket_name)

    original_filename = os.path.basename(file.name)
    # Create a unique path for the upload to avoid collisions
    gcs_object_name = f"gradio_uploads/{session_id}/{uuid.uuid4().hex[:8]}-{original_filename}"
    blob = bucket.blob(gcs_object_name)

    try:
        progress(0, desc="Starting Upload...")
        # For most STTMs, a direct upload is sufficient.
        blob.upload_from_filename(file.name)
        gcs_path = f"gs://{bucket_name}/{gcs_object_name}"
        print(f"File uploaded to {gcs_path}")

        # Update session state with the GCS path of the uploaded file
        session_state["uploaded_file_gcs_path"] = gcs_path

        gr.Info(f"File '{original_filename}' uploaded successfully!")
        # Return the original file path to display in the gr.File component
        return file.name, session_state
    except Exception as e:
        print(f"ERROR during file upload: {e}")
        gr.Warning(f"Failed to upload file: {e}")
        return None, session_state


async def chat_interface(message: str, history: list, session_state: Any):
    """
    Main function to handle the chat interaction with the agent.
    Streams the agent's response back to the Gradio UI.
    
    This function has been refactored to reuse the agent_runner
    across the entire conversation, preventing repeated welcome messages.
    """
    if not gcs_service:
        error_msg = "Agent backend is not initialized. Please check server logs for errors (e.g., missing GCP configuration)."
        history.append((message, error_msg))
        yield message, history, session_state, "Error", gr.update()
        return

    # Manage session state for the conversation
    if "user_id" not in session_state:
        session_state["user_id"] = f"user_{uuid.uuid4().hex[:8]}"
    if "session_id" not in session_state:
        session_state["session_id"] = f"session_{uuid.uuid4().hex[:8]}"

    user_id = session_state["user_id"]
    session_id = session_state["session_id"]

    # --- FIX: PERSIST AGENT RUNNER IN SESSION STATE ---
    # Check if a runner already exists for this session; if not, create one.
    if "agent_runner" not in session_state:
        print("Creating new agent session and runner.")
        session_service, _, session = await create_session(APP_NAME, user_id, session_id)
        session_state["agent_runner"] = create_runner(APP_NAME, root_agent, session_service, gcs_service)
    
    agent_runner = session_state["agent_runner"]
    
    uploaded_file_gcs_path = session_state.get("uploaded_file_gcs_path")
    initial_trigger_sent = session_state.get("initial_trigger_sent", False)

    if not message and not uploaded_file_gcs_path:
        yield message, history, session_state, "Idle", gr.update()
        return

    if uploaded_file_gcs_path and not initial_trigger_sent:
        task_instruction = message or "process the uploaded file to generate and run the dbt project."
        message_with_context = (
            f"A file has been uploaded to '{uploaded_file_gcs_path}'. "
            f"The user's instruction is: '{task_instruction}'. "
            "Please start the generation process now."
        )
        session_state["initial_trigger_sent"] = True
    else:
        message_with_context = message
    
    history.append((message, ""))
    yield "", history, session_state, "Processing...", gr.update()
    
    try:
        content = genai_types.Content(role="user", parts=[genai_types.Part(text=message_with_context)])
        run_config = RunConfig(streaming_mode=STREAMING_ENUM_VALUE)

        # This flag tracks if the agent's response should start a new bubble.
        # It's set to True after a tool is executed.
        start_new_bubble = False
        current_status = "Processing..."

        async for event in agent_runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=content,
            run_config=run_config,
        ):
            download_update = gr.update() # Default to no change

            # Process all parts of the event before yielding a single UI update.
            if hasattr(event, 'content') and event.content and event.content.parts:
                for part in event.content.parts:
                    if hasattr(part, 'text') and part.text:
                        text_part = part.text
                        step_match = re.search(r'(Step \d+ of \d+:.*)|(Validation Attempt.*)|(Attempting to fix.*)', text_part)
                        if step_match:
                            current_status = step_match.group(0).strip()

                        if start_new_bubble:
                            history.append((None, text_part))
                            start_new_bubble = False
                        else:
                            # This check prevents an error if the history is empty
                            if history and history[-1][0] is None:
                                history[-1] = (None, history[-1][1] + text_part)
                            else:
                                history.append((None, text_part))

                    elif hasattr(part, 'function_call') and part.function_call:
                        current_status = f"Executing tool: {part.function_call.name}"
                        start_new_bubble = True
                    
                    elif hasattr(part, 'function_response') and part.function_response:
                        tool_response = part.function_response
                        tool_name = tool_response.name
                        response_data = tool_response.response
                        
                        current_status = f"Tool '{tool_name}' finished. saving output..."
                        start_new_bubble = True

                        if isinstance(response_data, dict):
                            gcs_path = response_data.get('downloadable_gcs_path')
                            if gcs_path:
                                try:
                                    # (The download logic itself is correct)
                                    parsed_url = urlparse(gcs_path)
                                    bucket_name = parsed_url.netloc
                                    blob_name = parsed_url.path.lstrip('/')
                                    original_filename = os.path.basename(blob_name)
                                    with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{original_filename}") as tmp:
                                        local_temp_path = tmp.name
                                    
                                    blob = storage_client.bucket(bucket_name).blob(blob_name)
                                    blob.download_to_filename(local_temp_path)

                                    # If it's a test plan or report, rename it to a standard name for download.
                                    if "/test_plans/" in gcs_path or "/test_reports/" in gcs_path:
                                        new_filename = "test_report.csv" if "/test_reports/" in gcs_path else "test_cases.csv"
                                        new_local_path = os.path.join(os.path.dirname(local_temp_path), new_filename)
                                        os.rename(local_temp_path, new_local_path)
                                        download_file_path = new_local_path
                                    else:
                                        download_file_path = local_temp_path

                                    download_update = gr.update(value=download_file_path, visible=True, label=os.path.basename(download_file_path))
                                except Exception as e:
                                    print(f"ERROR: Failed to download file from GCS path {gcs_path}. Error: {e}")
            
            # Yield a single, consolidated update to the UI for this event.
            yield "", history, session_state, current_status, download_update

    except Exception as e:
        import traceback
        print(f"ERROR: An unexpected error occurred in chat_interface: {e}")
        error_message = f"An unexpected error occurred: {e}"
        history[-1] = (message, error_message)
        yield "", history, session_state, "Error", gr.update()

    # Final yield to reset status to Idle
    yield "", history, session_state, "Idle", gr.update()


def new_conversation_handler():
    """
    Clears all inputs, outputs, and state for a new conversation,
    including the agent_runner from the session state.
    """
    print("Clearing conversation state for new session.")
    return (
        gr.update(value=None),
        None,
        {},  # Resets the entire session_state
        gr.update(value=None, visible=False),
        "Idle",
        gr.update(value=None, visible=False) # Hide the test plan download component
    )


def build_gradio_app():
    """Builds and returns the Gradio application interface."""
    with gr.Blocks(theme=gr.themes.Soft(), title="DBT Agent Chat", fill_height=True) as demo:
        gr.Markdown("# 🤖 DBT Model Generator")
        gr.Markdown(
            "Welcome! I am an intelligent agent designed to streamline your dbt workflow. "
            "Simply upload a source-to-target mapping (STTM) file, "
            "and I will autonomously generate a complete, runnable dbt project for you. "
            "This includes creating models, schemas, and configuration files, followed by a validation run."
        )

        session_state = gr.State({})
        chatbot = gr.Chatbot(label="Conversation", bubble_full_width=False)
        status_bar = gr.Textbox(value="Idle", label="Agent Status", interactive=False)
        test_plan_display = gr.File(label="Download Test Plan", visible=False, interactive=False)

        with gr.Row(elem_id="file-upload-row"):
            with gr.Column(scale=1):
                upload_btn = gr.UploadButton(
                    "📎 Upload STTM",
                    file_types=[".csv", ".xlsx"],
                    variant="secondary"
                )
            with gr.Column(scale=4):
                file_display = gr.File(label="Uploaded File", visible=False)

        with gr.Row():
            msg_textbox = gr.Textbox(label="Your message", placeholder="Upload a file and/or type a message, e.g., 'Generate a dbt model'", scale=4)
            submit_btn = gr.Button("Send", variant="primary", scale=1)
        
        clear_btn = gr.Button("New Conversation")

        upload_btn.upload(handle_file_upload, [upload_btn, session_state], [file_display, session_state]).then(
            lambda: gr.update(visible=True), None, [file_display]
        )

        submit_btn.click(chat_interface, [msg_textbox, chatbot, session_state], [msg_textbox, chatbot, session_state, status_bar, test_plan_display])
        msg_textbox.submit(chat_interface, [msg_textbox, chatbot, session_state], [msg_textbox, chatbot, session_state, status_bar, test_plan_display])
        
        clear_btn.click(new_conversation_handler, None, [msg_textbox, chatbot, session_state, file_display, status_bar, test_plan_display], queue=False)

    return demo


if __name__ == "__main__":
    app = build_gradio_app()
    app.queue().launch(server_name="0.0.0.0", server_port=7860)