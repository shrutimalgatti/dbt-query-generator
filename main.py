import asyncio
import uuid
from google.genai import types as genai_types
from google.adk.agents.run_config import RunConfig, StreamingMode

from validation_tool_agent.setup.initialization import init_vertexai
from validation_tool_agent.agent import root_agent
from validation_tool_agent.services.session import create_session
from validation_tool_agent.services.runner import create_runner
import os
from dotenv import load_dotenv
load_dotenv()
from google.adk.artifacts import GcsArtifactService

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT")
GCP_LOCATION = os.environ.get("GCP_LOCATION")

gcs_bucket_name_py = "gs://shruti_test3"
try:
    gcs_service_py = GcsArtifactService(bucket_name=gcs_bucket_name_py)
    print(f"Python GcsArtifactService initialized for bucket: {gcs_bucket_name_py}")
except Exception as e:
    # Catch potential errors during GCS client initialization (e.g., auth issues)
    print(f"Error initializing Python GcsArtifactService: {e}")
try:
    if not GCP_PROJECT_ID:
        raise ValueError("GCP_PROJECT_ID is not set or is empty")
    if not GCP_LOCATION:
        raise ValueError("GCP_LOCATION is not set or is empty")
except ValueError as e: # Catch the ValueError we raised
    print(f"ERROR:{e}")
    
if GCP_PROJECT_ID and GCP_LOCATION:
    init_vertexai(GCP_PROJECT_ID,GCP_LOCATION)

APP_NAME = "dbt-query-generator"
USER_ID = f"user_{uuid.uuid4().hex[:8]}"

session_service, session_id, session = create_session(APP_NAME, USER_ID)
agent_runner = create_runner(APP_NAME, root_agent, session_service,gcs_bucket_name_py)

async def main():
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
        run_config = RunConfig(streaming_mode=StreamingMode.NONE)
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
                if event.tool_code_output:
                    print(f"Tool Output (stdout): {event.tool_code_output.stdout}")
                    print(f"Tool Output (stderr): {event.tool_code_output.stderr}")
                    # You might also want to print event.tool_code_output.result if the tool returns something
                    # print(f"Tool Return Value: {event.tool_code_output.result}") # Note: result might be a dict or object depending on your tool's return
                elif event.is_final_response():
                    if event.is_final_response():
                        if event.content and event.content.parts:
                            text_parts = [part.text for part in event.content.parts if hasattr(part, 'text') and part.text]
                            final_response_text = " ".join(text_parts).strip()
                        else:
                            final_response_text = "[Agent finished processing, but provided no textual response.]"
                    elif event.error_message:
                        final_response_text = f"Agent Error: {event.error_message}"
                        break

            print(f"Agent: {final_response_text}")

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