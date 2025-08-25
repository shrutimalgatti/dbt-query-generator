import asyncio
import uuid
from google.genai import types as genai_types
from google.adk.agents.run_config import RunConfig, StreamingMode
import os

from pathlib import Path
from dotenv import load_dotenv

# Build a path to the .env file in the project root directory.
# This makes the script independent of the current working directory.
dotenv_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path=dotenv_path)
from dbt_query_tool_agent.setup.initialization import init_vertexai
from google.adk.artifacts import GcsArtifactService

# --- Configuration ---
# Prioritize standard `GOOGLE_CLOUD_*` variables, but fall back to legacy `GCP_*` names
# for backward compatibility with older .env files.
GCP_PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
GCP_LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION") or os.environ.get("GCP_LOCATION")

# --- Initialization ---
# This must run before any agent modules are imported.
try:
    if not GCP_PROJECT_ID or not GCP_LOCATION:
        raise ValueError(
            "GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION environment variables must be set. "
            "Please check your .env file."
        )
    init_vertexai(GCP_PROJECT_ID, GCP_LOCATION)
    print(f"Vertex AI initialized for project '{GCP_PROJECT_ID}' in '{GCP_LOCATION}'.")
except ValueError as e:
    print(f"ERROR: {e}")
    exit(1) # Exit if configuration is missing

# Now that the environment is initialized, we can import our agent modules.
from dbt_query_tool_agent.agent import root_agent
from dbt_query_tool_agent.services.runner import create_runner
from dbt_query_tool_agent.services.session import create_session

# --- Robust StreamingMode detection ---
# Find a valid streaming mode attribute, trying common names to support different ADK versions.
if hasattr(StreamingMode, 'STREAMING'):
    STREAMING_ENUM_VALUE = StreamingMode.STREAMING
elif hasattr(StreamingMode, 'FINAL_RESPONSE_STREAM'):
    STREAMING_ENUM_VALUE = StreamingMode.FINAL_RESPONSE_STREAM
else:
    # As a last resort, fallback to non-streaming mode if no known streaming attribute is found.
    print("WARNING: Could not find a known streaming mode. Falling back to non-streaming mode.")
    STREAMING_ENUM_VALUE = StreamingMode.NONE

gcs_bucket_name_py = "gs://shruti_test3"
try:
    gcs_service_py = GcsArtifactService(bucket_name=gcs_bucket_name_py.replace("gs://", ""))
    print(f"Python GcsArtifactService initialized for bucket: {gcs_bucket_name_py}")
except Exception as e:
    # Catch potential errors during GCS client initialization (e.g., auth issues)
    print(f"Error initializing Python GcsArtifactService: {e}")

APP_NAME = "dbt-query-generator"
USER_ID = f"user_{uuid.uuid4().hex[:8]}"


async def main():
    session_service, session_id, session = await create_session(APP_NAME, USER_ID)
    agent_runner = create_runner(APP_NAME, root_agent, session_service, gcs_service_py)

    print("\n--- Simplified Agent Interaction ---")
    print(f"Session ID: {session.id}")
    print("Agent is ready. Type 'quit' to exit.")

    while True:
        try:
             user_query = await asyncio.to_thread(input, f"[{session.id}] : ")
        except RuntimeError:
            user_query = input(f"[{session.id}]")

        if user_query.lower() == 'quit':
            print("Exiting session.")
            break
        if not user_query:
            continue

        content = genai_types.Content(role='user', parts=[genai_types.Part(text=user_query)])
        run_config = RunConfig(streaming_mode=STREAMING_ENUM_VALUE)
        final_response_text = ""
        sttm_file_path = "sttm_mapping.csv"

        try:
            
            
            #  response_events = agent_runner.chat(
            #       user_query,
            #       file_attachments=[
            #          {'file_path': sttm_file_path, 'mime_type': 'text/csv'}
            #      ]
            #  )
            async for event in agent_runner.run_async(
                user_id=session.user_id,
                session_id=session.id,
                new_message=content,
                run_config=run_config
            ):
                if hasattr(event, 'tool_code_output') and event.tool_code_output:
                    if event.tool_code_output.stdout:
                        print(f"Tool Output (stdout): {event.tool_code_output.stdout}")
                    if event.tool_code_output.stderr:
                        print(f"Tool Output (stderr): {event.tool_code_output.stderr}")
                # Stream any text content from the agent to provide real-time feedback.
                elif event.content and event.content.parts and hasattr(event.content.parts[0], "text"):
                    text_part = event.content.parts[0].text
                    if text_part: # Prevent concatenating None
                        print(text_part, end="", flush=True) # Print immediately
                        final_response_text += text_part
                elif event.error_message:
                    final_response_text = f"Agent Error: {event.error_message}"
                    break

            if final_response_text:
                print() # Add a newline after the streamed response

        except Exception as e:
            print(f"\n Error during execution: {e}")
        print("-" * 25)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
    except Exception as main_ex:
        print(f"\n Unexpected error: {main_ex}")