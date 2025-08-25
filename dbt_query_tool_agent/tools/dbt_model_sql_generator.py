import io
import os
import re
from typing import Optional, List
import pandas as pd
from PIL import Image
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel
from google.adk.tools import FunctionTool
from dbt_query_tool_agent import prompts
from dbt_query_tool_agent.utils import infer_dbt_project_name_from_gcs_path

from google.cloud import storage
#PARSING_INSTRUCTIONS = prompts.PARSING_INSTRUCTIONS

def generate_dbt_model_sql(
    gcs_url: str,
    artifact_type: str = "model", # 'model', 'snapshot', 'macro', 'profiles_yml', 'schema_yml', 'test'
    unique_key: Optional[str] = None, 
    strategy: Optional[str] = None, 
    check_cols: Optional[str] = None, 
    updated_at_col: Optional[str] = None, 
    source_model_name: Optional[str] = None,
    schema_for_model: Optional[str] = None 
) -> dict:
    try:
        storage_client = storage.Client()
        if not gcs_url.startswith('gs://'):
            return {"error": "Invalid gcs URL"}
        
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
            return {'error': 'Object not available at input path'}
        
        bytes_content = blob.download_as_bytes()
        
        # --- Select specific prompt based on artifact_type ---
        # Use GENERAL_FORMATTING_INSTRUCTIONS as a base
        llm_prompt_parts = [prompts.GENERAL_PARSING_INSTRUCTIONS]

        specific_instruction = ""
        output_extension = ""
        dbt_folder = ""
        current_output_file_name = "" # Use a distinct variable for the output file name within the logic

        if artifact_type == "model":
            specific_instruction = prompts.DBT_MODEL_SQL_PROMPT
            dbt_folder = "dbt/models"
            output_extension = ".sql"
            current_output_file_name = base_file_name 
        elif artifact_type == "snapshot":
            specific_instruction = prompts.DBT_SNAPSHOT_SQL_PROMPT
            dbt_folder = "dbt/snapshots"
            output_extension = ".sql"

            if not source_model_name:
                return {"error": "source_model_name is required to create a snapshot."}
            
            current_output_file_name = f"{source_model_name}_snapshot"

            # Infer project and dataset for the target config
            project_id = os.environ.get("GOOGLE_CLOUD_PROJECT") or os.environ.get("GCP_PROJECT")
            if not project_id:
                return {"error": "Could not determine project ID from environment."}

            # Infer dataset name from STTM content to ensure consistency with profiles.yml
            sttm_blob = bucket.blob(blob_name)
            sttm_content_for_inference = sttm_blob.download_as_text() if file_type == '.csv' else ""
            datasetname = "your_default_dataset" # fallback
            if sttm_content_for_inference:
                inference_prompt = f"Read the following file content and extract the BigQuery dataset name from a fully qualified table name like 'project.dataset.table'. Only return the single dataset name and nothing else.\n\n{sttm_content_for_inference}"
                inference_response = model.generate_content(inference_prompt)
                datasetname = inference_response.text.strip()

            snapshot_details_prompt = f"""
            \nGenerate a dbt snapshot with the following configuration:
            - Snapshot Name: {current_output_file_name}
            - Source Model to reference: {source_model_name}
            - Unique Key: '{unique_key}'
            - Strategy: '{strategy}'
            - Target Database: '{project_id}'
            - Target Schema: '{datasetname}'
            """
            if strategy == 'check' and check_cols:
                snapshot_details_prompt += f"\n- Check Columns: {check_cols}"
            elif strategy == 'timestamp' and updated_at_col:
                snapshot_details_prompt += f"\n- Updated At Column: '{updated_at_col}'"
            
            specific_instruction += snapshot_details_prompt
        elif artifact_type == "macro":
            specific_instruction = prompts.DBT_MACRO_SQL_PROMPT
            dbt_folder = "dbt/macros"
            output_extension = ".sql" 
            current_output_file_name = base_file_name  
        elif artifact_type == "schema_yml":
            specific_instruction = prompts.DBT_SCHEMA_YML_PROMPT
            dbt_folder = "dbt/models" # schema.yml typically in models folder
            output_extension = ".yml"
            current_output_file_name = schema_for_model if schema_for_model else base_file_name # Name schema file after model if applicable
            if schema_for_model:
                specific_instruction += f"\nGenerate schema primarily for the model named '{schema_for_model}' and its associated sources."
        elif artifact_type == "test": # New artifact type for tests
            specific_instruction = prompts.DBT_TEST_SQL_PROMPT # Corrected prompt name
            dbt_folder = "dbt/tests"
            output_extension = ".sql"
            # Add the model name to the prompt instructions to prevent hallucination
            specific_instruction += f"\n\n**IMPORTANT**: The model being tested is named '{base_file_name}'. Use this name in all `ref()` macros."
            # For tests, the LLM will generate multiple SQL blocks.
            # The base_file_name here can be used as a prefix for test filenames.
            # The actual file names will be parsed from LLM output.

        llm_prompt_parts.append(specific_instruction)

        # Snapshots are generated based on user parameters, not the STTM file content.
        # For other artifacts, we include the STTM content for the LLM to parse.
        if artifact_type != "snapshot":
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
        
        output_paths: List[str] = []
        
        if artifact_type == "test":
            # Split content by the '---' delimiter for multiple test SQLs
            test_blocks = raw_generated_content.split('---')
            
            for block in test_blocks:
                block = block.strip()
                if not block:
                    continue

                # --- FIX: Use a more robust regex to parse the test block ---
                # This regex looks for the filename directive and captures the filename and the SQL that follows.
                match = re.search(r"output_file_name:\s*(?P<filename>[\w\.]+\.sql)\s*(?P<sql>.*)", block, re.DOTALL | re.IGNORECASE)

                if not match:
                    print(f"Warning: Could not find 'output_file_name:' in test block:\n{block}")
                    continue

                test_file_name_from_llm = match.group('filename').strip()
                sql_content = match.group('sql').strip().replace('```sql', '').replace('```', '').strip()

                if not sql_content:
                    continue
                
                # Use the filename provided by the LLM
                current_output_gcs_path = f"{dbt_project_name}/{dbt_folder}/{test_file_name_from_llm}"
                current_output_blob = bucket.blob(current_output_gcs_path)
                current_output_blob.metadata = {
                    'author': 'dbt_adk_agent', 
                    'dbt_artifact_type': 'test', 
                    'test_name': os.path.splitext(test_file_name_from_llm)[0], # Get name without extension
                    'original_source_file': file_name_with_ext
                }

                with current_output_blob.open('w') as file:
                    file.write(sql_content)
                
                output_paths.append(f'gs://{bucket_name}/{current_output_gcs_path}')
        else:
            # Existing logic for other single artifact types
            # --- FIX: Robustly parse the LLM output to extract only the SQL content ---
            # The LLM sometimes adds explanatory text before the code, often separated by '---'.
            # We take the last part after the final '---' to get the clean code.
            parts = raw_generated_content.split('---')
            clean_content = parts[-1].strip() # Get the last part and strip whitespace

            # Also remove markdown code fences
            generated_content = clean_content.replace('```sql', '').replace('```jinja', '').replace('```yaml', '').replace('```', '').strip()

            # --- REVISED FIX: Find the start of the actual SQL/Jinja code ---
            # The model might still include markdown headers. We find the first real SQL
            # keyword like 'WITH' or a dbt block like '{{' or '{%'.
            with_index = generated_content.find('WITH')
            config_index = generated_content.find('{{')
            jinja_block_index = generated_content.find('{%')

            # Find the minimum valid index (ignoring -1)
            indices = [i for i in [with_index, config_index, jinja_block_index] if i != -1]
            start_index = min(indices) if indices else -1
            
            if start_index != -1:
                generated_content = generated_content[start_index:]

            # If splitting resulted in an empty string, fallback to the original raw content
            if not generated_content:
                generated_content = raw_generated_content.replace('```sql', '').replace('```jinja', '').replace('```yaml', '').replace('```', '').strip()

            if dbt_folder:
                output_gcs_path = f"{dbt_project_name}/{dbt_folder}/{current_output_file_name}{output_extension}"
            else: 
                output_gcs_path = f"{dbt_project_name}/{current_output_file_name}{output_extension}"
                
            output_blob = bucket.blob(output_gcs_path)

            tags = {
                'author': 'dbt_adk_agent',
                'dbt_artifact_type': artifact_type,
                'original_source_file': file_name_with_ext
            }
            output_blob.metadata = tags

            with output_blob.open('w') as file:
                file.write(generated_content)
            
            output_paths.append(f'gs://{bucket_name}/{output_gcs_path}')

        return {
            'output_path': output_paths, # Always return a list of paths
            'output_sql': raw_generated_content, # Return raw_generated_content for all types for debugging if needed
            'result': 'SUCCESS'
        }
    except Exception as err:
        import traceback
        traceback.print_exc() # Print full stack trace for debugging
        return {
            'output_path': [],
            'output_sql': '',
            'result': 'ERROR',
            'message': str(err)
        }

# Rename the tool for clarity as it handles multiple artifact types
generate_dbt_model_sql_tool = FunctionTool(generate_dbt_model_sql) # Keep the function name consistent with the tool name if you renamed it in agent.py