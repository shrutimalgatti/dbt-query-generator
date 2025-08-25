import os
from typing import Optional
from vertexai.generative_models import GenerativeModel
from urllib.parse import urlparse
from google.adk.tools import FunctionTool
from dbt_query_tool_agent import prompts
from dbt_query_tool_agent.utils import infer_dbt_project_name_from_gcs_path

from google.cloud import storage
STORAGE_CLIENT = storage.Client()
MODEL = 'gemini-2.5-flash'

def generate_dbt_project_yml(
    gcs_url: str, # GCS URL for the project root, e.g., gs://my-bucket/my-project/
) -> dict:
    """
    Generates a dbt_project.yml file and saves it to the root of the dbt project in GCS.
    This tool does not require a source-to-target mapping file.
    """
    try:
        if not gcs_url.startswith('gs://'):
            return {"error": "Invalid GCS URL. Must start with 'gs://'."}

        parsed_url = urlparse(gcs_url)
        bucket_name = parsed_url.netloc
        dbt_project_name = infer_dbt_project_name_from_gcs_path(gcs_url)

        if not dbt_project_name:
            return {"error": "Could not determine dbt_project_name from GCS URL."}

        bucket = STORAGE_CLIENT.bucket(bucket_name)
        model = GenerativeModel(MODEL)

        # The prompt is self-contained and uses the project name.
        llm_prompt = [
            prompts.GENERAL_PARSING_INSTRUCTIONS,
            prompts.DBT_PROJECT_YML_PROMPT,
            f"\nGenerate the dbt_project.yml content for a project named: '{dbt_project_name}'"
        ]

        response = model.generate_content(llm_prompt)

        # Extract the generated YAML content
        output_yml = response.text.replace('```yaml', '').replace('```', '').strip()

        # Construct the full output GCS path for dbt_project.yml
        output_gcs_path = f"{dbt_project_name}/dbt/dbt_project.yml"
        output_blob = bucket.blob(output_gcs_path)

        tags = {
            'author': 'dbt_adk_agent',
            'dbt_artifact_type': 'dbt_project_yml'
        }
        output_blob.metadata = tags

        with output_blob.open('w') as file:
            file.write(output_yml)

        return {
            'output_path': f'gs://{bucket_name}/{output_gcs_path}',
            'result': 'SUCCESS'
        }
    except Exception as err:
        return {
            'result': 'ERROR',
            'message': str(err)
        }

generate_dbt_project_yml_tool = FunctionTool(generate_dbt_project_yml)
