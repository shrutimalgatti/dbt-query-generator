import io
import os
from typing import Optional
from PIL import Image
import google.generativeai as genai
import vertexai
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.adk.tools import FunctionTool
from dbt_query_tool_agent import prompts

from google.cloud import storage
STORAGE_CLIENT = storage.Client()
MODEL = 'gemini-2.5-flash'

#PARSING_INSTRUCTIONS = prompts.PARSING_INSTRUCTIONS

def generate_dbt_model_sql(
    gcs_url: str,
    artifact_type: str = "model", # 'model', 'snapshot', 'macro', 'profiles_yml', 'schema_yml'
    unique_key: Optional[str] = None, 
    strategy: Optional[str] = None, 
    check_cols: Optional[str] = None, 
    updated_at_col: Optional[str] = None, 
    source_model_name: Optional[str] = None,
    # New parameter to specify which model a schema.yml refers to
    schema_for_model: Optional[str] = None 
) -> dict:
    try:
        if not gcs_url.startswith('gs://'):
            return {"error": "Invalid gcs URL"}
        
        parsed_url = urlparse(gcs_url)
        bucket_name = parsed_url.netloc
        file_path = parsed_url.path.lstrip('/')
        
        bucket_name, file_path = gcs_url[5:].split('/', 1)
        dbt_project_name = file_path.split('/')[0]
        file_name = file_path.split('/')[-1].split('.')[0]
        file_type = file_path.split('/')[-1].split('.')[1]

        # Ensure dbt_project_name is extracted correctly
        #path_parts = file_path.split('/')
        #if len(path_parts) > 1:
        #    dbt_project_name = path_parts[0]
        #else:
        #    dbt_project_name = "default_dbt_project" 
            
        # Extract file name without extension for naming the dbt artifact
        file_name_with_ext = os.path.basename(file_path)
        base_file_name = os.path.splitext(file_name_with_ext)[0]
        file_type = os.path.splitext(file_name_with_ext)[1].lstrip('.')
        
        bucket = STORAGE_CLIENT.bucket(bucket_name)
        blob = bucket.blob(file_path)

        model = genai.GenerativeModel(MODEL)

        if not blob.exists():
            return {'error': 'Object not available at input path'}
        
        bytes_content = blob.download_as_bytes()
        
        # --- Crucial Change: Select specific prompt based on artifact_type ---
        llm_prompt_parts = [prompts.GENERAL_PARSING_INSTRUCTIONS] # Always include general format instructions

        specific_instruction = ""
        if artifact_type == "model":
            specific_instruction = prompts.DBT_MODEL_SQL_PROMPT
        elif artifact_type == "snapshot":
            specific_instruction = prompts.DBT_SNAPSHOT_SQL_PROMPT
            specific_instruction += f"""
            The source model to be snapshotted is: {source_model_name if source_model_name else base_file_name}.
            Unique key: '{unique_key}'. Strategy: '{strategy}'.
            """
            if strategy == 'check' and check_cols:
                specific_instruction += f"Monitor columns: {check_cols}."
            elif strategy == 'timestamp' and updated_at_col:
                specific_instruction += f"Updated at column: '{updated_at_col}'."
        elif artifact_type == "macro":
            specific_instruction = prompts.DBT_MACRO_SQL_PROMPT
        elif artifact_type == "profiles_yml":
            specific_instruction = prompts.DBT_PROFILES_YML_PROMPT
        elif artifact_type == "schema_yml":
            specific_instruction = prompts.DBT_SCHEMA_YML_PROMPT
            if schema_for_model:
                specific_instruction += f"\nGenerate schema for model: '{schema_for_model}'."
        elif artifact_type == "test": # New artifact type for tests
            specific_instruction = prompts.DBT_TEST_SQL_PROMPT
            # Ensure the prompt knows which model these tests are for
            specific_instruction += f"\nGenerate tests for target model: '{schema_for_model if schema_for_model else base_file_name}'. Analyze the provided mapping to infer test scenarios."


        llm_prompt_parts.append(specific_instruction)

        if file_type == 'csv':
            file_content = bytes_content.decode('utf-8')
            llm_prompt_parts.append(f"\n--- Input CSV Content for Schema/Model/Test Inference ---\n{file_content}\n--- End Input CSV Content ---")
        else: # Assuming other types are images
            image = Image.open(io.BytesIO(bytes_content))
            llm_prompt_parts.append(image)

        response = model.generate_content(llm_prompt_parts)

        raw_generated_content = response.text.strip()
        
        output_paths: List[str] = []
        
        if artifact_type == "test":
            # Split the generated content by the '---output_file_name:' delimiter
            # and process each test individually.
            test_parts = raw_generated_content.split('---')
            
            for part in test_parts:
                if 'output_file_name:' in part:
                    lines = part.strip().split('\n')
                    # Extract the filename from the line that contains 'output_file_name:'
                    file_name_line = next((line for line in lines if 'output_file_name:' in line), None)
                    if file_name_line:
                        output_file_name = file_name_line.split('output_file_name:')[1].strip()
                        # The SQL content is everything after the file name line
                        sql_content = "\n".join(lines[lines.index(file_name_line) + 1:]).strip()
                        
                        dbt_folder = "tests"
                        output_extension = ".sql"
                        
                        output_gcs_path = f"{dbt_project_name}/{dbt_folder}/{output_file_name}"
                        output_blob = bucket.blob(output_gcs_path)

                        tags = {
                            'author': 'dbt_adk_agent',
                            'dbt_artifact_type': artifact_type,
                            'original_source_file': os.path.basename(file_path)
                        }
                        output_blob.metadata = tags

                        with output_blob.open('w') as file:
                            file.write(sql_content)
                        
                        output_paths.append(f'gs://{bucket_name}/{output_gcs_path}')
        else:
            # Existing logic for other artifact types
            generated_content = raw_generated_content.replace('```sql', '').replace('```jinja', '').replace('```yaml', '').replace('```', '').strip()

            dbt_folder = ""
            output_extension = ""
            output_file_name = ""

            if artifact_type == "model":
                dbt_folder = "models"
                output_extension = ".sql"
                output_file_name = base_file_name
            elif artifact_type == "snapshot":
                dbt_folder = "snapshots"
                output_extension = ".sql"
                output_file_name = f"{base_file_name}_snapshot"
            elif artifact_type == "macro":
                dbt_folder = "macros"
                output_extension = ".sql"
                output_file_name = base_file_name
            elif artifact_type == "profiles_yml":
                dbt_folder = "" # profiles.yml sits at the root of the project
                output_extension = ".yml"
                output_file_name = "profiles"
            elif artifact_type == "schema_yml":
                dbt_folder = "models"
                output_extension = ".yml"
                output_file_name = schema_for_model if schema_for_model else base_file_name
            
            if dbt_folder:
                output_gcs_path = f"{dbt_project_name}/{dbt_folder}/{output_file_name}{output_extension}"
            else:
                output_gcs_path = f"{dbt_project_name}/{output_file_name}{output_extension}"

            output_blob = bucket.blob(output_gcs_path)

            tags = {
                'author': 'dbt_adk_agent',
                'dbt_artifact_type': artifact_type
            }
            output_blob.metadata = tags

            with output_blob.open('w') as file:
                file.write(generated_content)
            
            output_paths.append(f'gs://{bucket_name}/{output_gcs_path}')

        return {
            'output_path': output_paths, # Now returns a list of paths
            'output_sql': raw_generated_content if artifact_type == "test" else generated_content, # Keep raw content for tests for debugging
            'result': 'SUCCESS'
        }
    except Exception as err:
        return {
            'output_path': [],
            'output_sql': '',
            'result': 'ERROR',
            'message': str(err)
        }

generate_dbt_model_sql_tool = FunctionTool(generate_dbt_model_sql)