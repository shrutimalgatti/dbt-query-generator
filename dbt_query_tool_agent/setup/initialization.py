import os
import vertexai
from dotenv import load_dotenv
import pathlib # For robust path manipulation

def init_vertexai(GCP_PROJECT_ID,GCP_LOCATION):
    try:
        vertexai.init(project=GCP_PROJECT_ID, location=GCP_LOCATION)
        print("Vertex AI SDK initialized.")
    except Exception as e:
        print(f"Could not initialize Vertex AI SDK: {e}")