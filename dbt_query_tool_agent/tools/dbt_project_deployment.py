import os
import vertexai
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.adk.tools import FunctionTool
from google.cloud import storage
STORAGE_CLIENT = storage.Client()
MODEL = 'gemini-2.5-flash'

def deploy_dbt_project(gcs_bucket_path: str) -> dict:
    try:
        if not gcs_bucket_path.startswith('gs://'):
            return "Invalid gcs URL"
        
        bucket_name, project_name = gcs_bucket_path[5:].split('/', 1)

        bucket = STORAGE_CLIENT.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=project_name)

        # COMPUTING THE TARGET FOLDER
        target_folder = f'dbt_projects/{project_name}'

        # CREATING TARGET DIRECTORY IF NOT EXIST
        os.makedirs(target_folder, exist_ok=True)

        for blob in blobs:
            relative_path = os.path.relpath(blob.name, project_name)
            local_file_path = os.path.join(target_folder, relative_path)
            local_file_dir = os.path.dirname(local_file_path)

            # CREATE SUB-DIRECTORY IF NOT EXIST
            os.makedirs(local_file_dir, exist_ok=True)

            # DOWNLOAD THE FILE TO THE PATH
            if not blob.name.endswith('/'):
                blob.download_to_filename(local_file_path)
        return {
                'deployment_status': 'success',
                'deployed_path': f'./dbt_projects/{project_name}'
            }
    except Exception as err:
        return {
            'deployment_status': f'error - {str(err)}',
            'deployed_path': None
        }
deploy_dbt_project_tool = FunctionTool(deploy_dbt_project)