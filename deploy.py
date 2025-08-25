import vertexai
from vertexai import agent_engines
from pathlib import Path
from dotenv import load_dotenv
import os

# Build a path to the .env file in the project root directory.
# This makes the script independent of the current working directory.
dotenv_path = Path(__file__).resolve().parent / '.env'
load_dotenv(dotenv_path=dotenv_path)

# Import the centralized initialization function
from dbt_query_tool_agent.setup.initialization import init_vertexai

# Use standardized environment variables
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION") or os.environ.get("GCP_LOCATION")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET")

# Add explicit checks for required variables before proceeding.
if not PROJECT_ID or not LOCATION or not STAGING_BUCKET:
    raise ValueError(
        "Missing required environment variables. Please ensure GOOGLE_CLOUD_PROJECT, "
        "GOOGLE_CLOUD_LOCATION, and STAGING_BUCKET are set in your .env file."
    )

# Initialize Vertex AI *before* importing any agent modules.
# Use the centralized init function to ensure all SDKs are configured correctly.
init_vertexai(
    project_id=PROJECT_ID,
    location=LOCATION,
    staging_bucket=STAGING_BUCKET
)

# Now that the SDK is initialized, import the agent.
from dbt_query_tool_agent.agent import root_agent

remote_app = agent_engines.create(
    agent_engine=root_agent,
    requirements=[
        "google-cloud-aiplatform[adk,agent_engines]",
    ]
)