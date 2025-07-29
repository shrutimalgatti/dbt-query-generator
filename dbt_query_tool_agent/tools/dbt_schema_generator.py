import io
import os
from PIL import Image
import google.generativeai as genai
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.adk.tools import FunctionTool
from dbt_query_tool_agent import prompts
from typing import Optional

from google.cloud import storage
STORAGE_CLIENT = storage.Client()
MODEL = 'gemini-2.5-flash'

SCHEMA_YML_PROMPT_INSTRUCTIONS = prompts.DBT_SCHEMA_YML_PROMPT

def generate_dbt_schema_yml(
    gcs_url: str, # GCS URL to the source-to-target mapping (CSV or Image)
    dbt_project_name: Optional[str] = None # Optional: user can provide if not inferrable from GCS URL
) -> dict:
    """
    Generates a dbt schema.yml file from a source-to-target mapping
    and saves it to the 'models/' folder within the dbt project in GCS.
    """
    try:
        if not gcs_url.startswith('gs://'):
            return {"error": "Invalid GCS URL. Must start with 'gs://'."}
        
        parsed_url = urlparse(gcs_url)
        bucket_name = parsed_url.netloc
        file_path = parsed_url.path.lstrip('/')

        # Infer dbt_project_name if not provided
        if not dbt_project_name:
            # Assuming the dbt project name is the first part of the file_path
            # Example: gs://my-bucket/my_dbt_project/data/input.csv -> dbt_project_name = 'my_dbt_project'
            dbt_project_name = file_path.split('/')[0]
            if not dbt_project_name: # Fallback if path is just a file at root
                return {"error": "Could not infer dbt project name from GCS URL. Please provide it."}

        bucket = STORAGE_CLIENT.bucket(bucket_name)
        blob = bucket.blob(file_path)

        model = genai.GenerativeModel(MODEL)

        if not blob.exists():
            return {'error': 'Object not available at input path'}
        
        bytes_content = blob.download_as_bytes()
        
        file_type = os.path.splitext(os.path.basename(file_path))[1].lstrip('.')

        # Prepare the prompt for the LLM to generate schema.yml
        llm_prompt_parts = [
            SCHEMA_YML_PROMPT_INSTRUCTIONS,
            f"""
            From the following source-to-target mapping, generate a dbt `schema.yml` file.
            Include `sources` with their tables and columns, and `models` with their columns.
            For descriptions, use generic placeholders or infer based on common data practices.
            Ensure the output is in YAML format within a markdown code block.
            """
        ]

        # Add input content (CSV or Image)
        if file_type == 'csv':
            file_content = bytes_content.decode('utf-8')
            llm_prompt_parts.append(f"\n--- Input CSV Content ---\n{file_content}\n--- End Input CSV Content ---")
        else: # Assuming other types are images
            image = Image.open(io.BytesIO(bytes_content))
            llm_prompt_parts.append(image)

        response = model.generate_content(llm_prompt_parts)

        # Extract the generated YAML content
        output_yml = response.text.replace('```yaml', '').replace('```', '').strip()

        # Construct the full output GCS path for schema.yml
        # It's typically placed in the models folder
        output_gcs_path = f"{dbt_project_name}/models/schema.yml"
        output_blob = bucket.blob(output_gcs_path)

        tags = {
            'author': 'dbt_adk_agent',
            'dbt_artifact_type': 'schema_yml'
        }
        output_blob.metadata = tags

        with output_blob.open('w') as file:
            file.write(output_yml)

        return {
            'output_path': f'gs://{bucket_name}/{output_gcs_path}',
            'output_yml_content': output_yml,
            'result': 'SUCCESS'
        }
    except Exception as err:
        return {
            'output_path': '',
            'output_yml_content': '',
            'result': 'ERROR',
            'message': str(err)
        }

generate_dbt_schema_yml_tool = FunctionTool(generate_dbt_schema_yml)
