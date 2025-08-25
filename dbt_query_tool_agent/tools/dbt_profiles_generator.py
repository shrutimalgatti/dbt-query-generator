import os
from urllib.parse import urlparse
from google.adk.tools import FunctionTool
from dbt_query_tool_agent import prompts
from google.cloud import storage
import pandas as pd
from vertexai.generative_models import GenerativeModel
import io
from dbt_query_tool_agent.utils import infer_dbt_project_name_from_gcs_path
from PIL import Image

STORAGE_CLIENT = storage.Client()
MODEL = 'gemini-2.5-flash'

def generate_dbt_profiles_yml(
    gcs_sttm_url: str
) -> dict:
    """
    Generates a dbt profiles.yml file by inferring details and saves it to GCS.

    This tool autonomously infers the necessary details:
    1.  It reads the Google Cloud Project ID from the environment variables.
    2.  It infers the dbt project name from the GCS URL of the STTM file.
    3.  It inspects the STTM file content (CSV or image) to infer the BigQuery
        dataset name from table identifiers (e.g., 'project.dataset.table').

    Args:
        gcs_sttm_url (str): The GCS URL of the source-to-target mapping (STTM)
                            file (e.g., 'gs://your-bucket/sttm_file.csv').

    Returns:
        dict: A dictionary containing the GCS path of the generated profiles.yml
              file and the content of the file.
    """
        
    try:
        print(f"--- Executing Tool: generate_dbt_profiles_yml for GCS URL: {gcs_sttm_url} ---")
        if not gcs_sttm_url.startswith('gs://'):
            return {"error": "Invalid GCS URL. Must start with 'gs://'."}
        
        # 1. Get Project ID from environment
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
        if not project_id:
            return {"error": "ERROR: GOOGLE_CLOUD_PROJECT or GCP_PROJECT environment variable not set."}

        parsed_url = urlparse(gcs_sttm_url)
        bucket_name = parsed_url.netloc
        sttm_blob_name = parsed_url.path.lstrip('/')
        file_type = os.path.splitext(sttm_blob_name)[1].lower()

        # 2. Infer dataset name from STTM content by calling the LLM
        sttm_blob = STORAGE_CLIENT.bucket(bucket_name).blob(sttm_blob_name)
        if not sttm_blob.exists():
            return {"error": f"The specified STTM file does not exist at {gcs_sttm_url}"}

        model_for_inference = GenerativeModel(MODEL)
        inference_prompt_parts = [
            "Read the following file content and extract the BigQuery dataset name from a fully qualified table name like 'project.dataset.table'. Only return the single dataset name and nothing else."
        ]
        if file_type == '.csv':
            inference_prompt_parts.append(sttm_blob.download_as_text())
        elif file_type == '.xlsx':
            df = pd.read_excel(io.BytesIO(sttm_blob.download_as_bytes()))
            inference_prompt_parts.append(df.to_csv(index=False))
        else: # Assume image for other types
            try:
                image_bytes = sttm_blob.download_as_bytes()
                inference_prompt_parts.append(Image.open(io.BytesIO(image_bytes)))
            except Image.UnidentifiedImageError:
                return {"error": f"Unsupported file type: '{file_type}'. Please upload a CSV, XLSX, or a valid image file."}

        inference_response = model_for_inference.generate_content(inference_prompt_parts)
        dataset_name = inference_response.text.strip()

        # 3. Infer dbt project name from GCS path
        dbt_project_name = infer_dbt_project_name_from_gcs_path(gcs_sttm_url)

        bucket = STORAGE_CLIENT.bucket(bucket_name)
        
        threads=1
        timeout_seconds=300

        # Craft the prompt for the LLM to generate profiles.yml
        llm_prompt_parts = [
            prompts.GENERAL_PARSING_INSTRUCTIONS,
            prompts.DBT_PROFILES_YML_PROMPT,
            f"""
            Use the following details:
            - DBT Project Name: {dbt_project_name}
            - BigQuery Project ID: {project_id}
            - BigQuery Dataset Name: {dataset_name}
            - Threads: {threads}
            - Timeout Seconds: {timeout_seconds}
            
            Ensure the output is in YAML format within a markdown code block.
            """
        ]

        response = model_for_inference.generate_content(llm_prompt_parts)

        # Extract the generated YAML content
        output_yml = response.text.replace('```yaml', '').replace('```', '').strip()

        # --- FIX: Programmatically correct common LLM errors ---
        # The LLM sometimes incorrectly uses 'job_timeout_ms' instead of 'timeout_seconds'.
        # This replacement ensures the generated profile is always valid for dbt-bigquery.
        output_yml = output_yml.replace('job_timeout_ms', 'timeout_seconds')

        # Construct the full output GCS path for profiles.yml (at the root of the dbt project)
        output_gcs_path = f"{dbt_project_name}/dbt/profiles.yml"
        output_blob = bucket.blob(output_gcs_path)

        tags = {
            'author': 'dbt_adk_agent',
            'dbt_artifact_type': 'profiles_yml'
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
        traceback.print_exc()
        return {
            'output_path': '',
            'output_yml_content': '',
            'result': 'ERROR',
            'message': str(err)
        }

generate_dbt_profiles_yml_tool = FunctionTool(generate_dbt_profiles_yml)