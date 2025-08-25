import io
import os
import pandas as pd
from PIL import Image
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel
from google.adk.tools import FunctionTool
from dbt_query_tool_agent import prompts
from typing import Optional
from dbt_query_tool_agent.utils import infer_dbt_project_name_from_gcs_path

from google.cloud import storage
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
        print(f"--- Executing Tool: generate_dbt_schema_yml for GCS URL: {gcs_url} ---")
        storage_client = storage.Client()
        if not gcs_url.startswith('gs://'):
            return {"error": "Invalid GCS URL. Must start with 'gs://'."}
        
        parsed_url = urlparse(gcs_url)
        bucket_name = parsed_url.netloc
        # This blob_name correctly represents the path without the bucket.
        blob_name = parsed_url.path.lstrip('/') # e.g. 'gradio_uploads/.../file.csv'

        # Infer project name from the original filename embedded in the GCS path
        inferred_project_name = infer_dbt_project_name_from_gcs_path(gcs_url)
        # Use provided name, otherwise use the inferred one.
        final_dbt_project_name = dbt_project_name or inferred_project_name

        if not final_dbt_project_name:
             return {"error": "Could not determine dbt project name from GCS URL."}

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        model = GenerativeModel('gemini-2.5-flash')

        if not blob.exists():
            return {'error': 'Object not available at input path'}
        
        bytes_content = blob.download_as_bytes()
        
        # --- FIX: Prepare the prompt using specific prompts module variables ---
        llm_prompt_parts = [
            #prompts.GENERAL_FORMATTING_INSTRUCTIONS, # Ensures consistent output formatting
            prompts.DBT_SCHEMA_YML_PROMPT, # The detailed schema instructions
            f"\nGenerate the schema for a model named: '{final_dbt_project_name}'"
        ]

        # Add input content (CSV or Image)
        file_type = os.path.splitext(blob_name)[1].lower()
        if file_type == '.csv':
            file_content = bytes_content.decode('utf-8')
            llm_prompt_parts.append(f"\n--- Input CSV Content for Schema Inference ---\n{file_content}\n--- End Input CSV Content ---")
        elif file_type == '.xlsx':
            df = pd.read_excel(io.BytesIO(bytes_content))
            csv_string = df.to_csv(index=False)
            llm_prompt_parts.append(f"\n--- Input Excel (converted to CSV) Content for Schema Inference ---\n{csv_string}\n--- End Input Excel Content ---")
        else: # Assume image for other types
            try:
                image = Image.open(io.BytesIO(bytes_content))
                llm_prompt_parts.append(image)
            except Image.UnidentifiedImageError:
                return {"error": f"Unsupported file type: '{file_type}'. Please upload a CSV, XLSX, or a valid image file."}

        response = model.generate_content(llm_prompt_parts)

        # --- FIX: Robustly parse the LLM output to extract only the YAML content ---
        raw_text = response.text
        
        # Find the start of the actual YAML content, which is usually `version: 2`.
        # This handles cases where the LLM adds explanatory text before the code block.
        yaml_start_index = raw_text.find('version: 2')
        if yaml_start_index != -1:
            # Slice from the start of the YAML content
            output_yml = raw_text[yaml_start_index:]
        else:
            # Fallback if 'version: 2' is not found
            output_yml = raw_text

        # Clean up any remaining markdown code fences
        output_yml = output_yml.replace('```yaml', '').replace('```', '').strip()

        # Construct the full output GCS path for schema.yml
        output_gcs_path = f"{final_dbt_project_name}/dbt/models/schema.yml" # Consistent path
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
        import traceback
        traceback.print_exc() # Print full stack trace for debugging
        return {
            'output_path': '',
            'output_yml_content': '',
            'result': 'ERROR',
            'message': str(err)
        }

generate_dbt_schema_yml_tool = FunctionTool(generate_dbt_schema_yml)