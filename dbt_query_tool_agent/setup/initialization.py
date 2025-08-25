import vertexai
import google.generativeai as genai
import os
from typing import Optional


def init_vertexai(
    project_id: str, location: str, staging_bucket: Optional[str] = None
):
    """
    Initializes the Vertex AI SDK and configures the google.generativeai
    library to use Vertex AI as its backend.

    This ensures that both the Vertex AI SDK (`vertexai.*`) and the
    Google AI SDK (`google.generativeai.*`), which is used by the ADK's
    `LlmAgent`, are correctly configured to use the same GCP project
    and authentication method (Application Default Credentials).
    """
    # This is the primary initialization for the Vertex AI SDK.
    vertexai.init(project=project_id, location=location, staging_bucket=staging_bucket)

    # Explicitly configure the google.generativeai library to use the
    # Vertex AI transport layer. This is more robust than relying on
    # environment variables alone, as it prevents the library from
    # accidentally trying to use a GOOGLE_API_KEY if one is present
    # in the environment.
    genai.configure(transport="vertex_ai")