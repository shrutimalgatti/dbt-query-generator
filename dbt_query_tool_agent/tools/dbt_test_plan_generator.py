import io
import os
from typing import Optional, List
from PIL import Image
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel
from google.adk.tools import FunctionTool
# Assuming prompts.py is accessible in the same module path
from dbt_query_tool_agent import prompts
from dbt_query_tool_agent.utils import infer_dbt_project_name_from_gcs_path
import pandas as pd
from google.cloud import storage

def generate_dbt_test_case_sheet(
    gcs_url: str,
    output_format: str = "csv" # Can be 'csv' or 'xlsx' (requires openpyxl setup)
) -> dict:
    """
    Generates a DBT test case sheet in the specified format (CSV or XLSX)
    from a source-to-target mapping file (image/CSV) located at a GCS URL.
    """
    try:
        storage_client = storage.Client()
        if not gcs_url.startswith('gs://'):
            return {"error": "Invalid GCS URL. Please provide a path starting with gs://"}

        parsed_url = urlparse(gcs_url)
        bucket_name = parsed_url.netloc
        blob_name = parsed_url.path.lstrip('/')

        dbt_project_name = infer_dbt_project_name_from_gcs_path(gcs_url)

        file_name_with_ext = os.path.basename(blob_name)
        # The base_file_name should be the clean name inferred from the original
        # filename, not the one with the UUID prefix.
        base_file_name = dbt_project_name
        file_type = os.path.splitext(file_name_with_ext)[1].lower()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        model = GenerativeModel('gemini-2.5-flash')

        if not blob.exists():
            return {'error': f'Object not available at input path: {gcs_url}'}

        bytes_content = blob.download_as_bytes()

        llm_prompt_parts = [prompts.GENERAL_PARSING_INSTRUCTIONS]
        llm_prompt_parts.append(prompts.DBT_TEST_CASE_SHEET_PROMPT) # Use the specific prompt

        if file_type == '.csv':
            file_content = bytes_content.decode('utf-8')
            llm_prompt_parts.append(f"\n--- Input CSV Content for Inference ---\n{file_content}\n--- End Input CSV Content ---")
        elif file_type == '.xlsx':
            df = pd.read_excel(io.BytesIO(bytes_content))
            csv_string = df.to_csv(index=False)
            llm_prompt_parts.append(f"\n--- Input Excel (converted to CSV) Content for Inference ---\n{csv_string}\n--- End Input Excel Content ---")
        else: # Assume image for other types
            try:
                image = Image.open(io.BytesIO(bytes_content))
                llm_prompt_parts.append(image)
            except Image.UnidentifiedImageError:
                return {"error": f"Unsupported file type: '{file_type}'. Please upload a CSV, XLSX, or a valid image file."}

        response = model.generate_content(llm_prompt_parts)
        raw_generated_content = response.text.strip()

        # Define output folder and file name
        dbt_folder = "test_plans"
        current_output_file_name = f"{base_file_name}_test_cases"
        output_paths: List[str] = []

        if output_format == "csv":
            output_extension = ".csv"
            try:
                # Attempt to parse as CSV; if it fails, save raw content
                df = pd.read_csv(io.StringIO(raw_generated_content))
                final_content = df.to_csv(index=False)
            except Exception as e:
                print(f"Warning: LLM did not produce perfect CSV for test case sheet. Saving raw text. Error: {e}")
                final_content = raw_generated_content # Fallback to raw text if CSV parsing fails
        elif output_format == "xlsx":
            output_extension = ".xlsx"
            # This part requires openpyxl. Without it, this will fail.
            # You'd typically need to ensure 'openpyxl' is installed in the environment.
            try:
                # Attempt to parse as CSV first, then convert to XLSX
                df = pd.read_csv(io.StringIO(raw_generated_content))
                output_buffer = io.BytesIO()
                with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Test Cases')
                final_content = output_buffer.getvalue() # Get bytes for XLSX
            except ImportError:
                return {'error': 'openpyxl is not installed. Cannot generate XLSX. Please use CSV format or install openpyxl.', 'result': 'ERROR'}
            except Exception as e:
                print(f"Warning: Failed to convert to XLSX. Saving raw text as CSV instead. Error: {e}")
                # Fallback to CSV if XLSX conversion fails for other reasons
                output_extension = ".csv"
                final_content = raw_generated_content.encode('utf-8') # Ensure bytes for GCS write
        else:
            return {'error': 'Unsupported output format. Please choose "csv" or "xlsx".', 'result': 'ERROR'}

        output_gcs_path = f"{dbt_project_name}/{dbt_folder}/{current_output_file_name}{output_extension}"
        output_blob = bucket.blob(output_gcs_path)

        output_blob.metadata = {
            'author': 'dbt_adk_agent',
            'dbt_artifact_type': 'test_case_sheet',
            'original_source_file': file_name_with_ext
        }

        # Handle writing based on content type (bytes for XLSX, string for CSV)
        if output_format == "xlsx":
            output_blob.upload_from_string(final_content, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        else: # CSV
            output_blob.upload_from_string(final_content, content_type='text/csv')

        output_paths.append(f'gs://{bucket_name}/{output_gcs_path}')

        return {
            'downloadable_gcs_path': output_paths[0] if output_paths else None,
            'raw_llm_output': raw_generated_content, # Useful for debugging LLM's raw response
            'result': 'SUCCESS'
        }
    except Exception as err:
        import traceback
        traceback.print_exc()
        return {
            'output_path': [],
            'raw_llm_output': '',
            'result': 'ERROR',
            'message': str(err)
        }

dbt_test_case_generator_tool = FunctionTool(generate_dbt_test_case_sheet)