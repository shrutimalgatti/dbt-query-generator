from google.adk.agents import Agent
from google.cloud import storage
import google.generativeai as genai
import io
import os
import warnings
import subprocess

def deploy_dbt_project(gcs_bucket_path: str) -> dict:
    try:
        project_name = gcs_bucket_path.split('/')[-1]
        run_status = os.system(
            f'''
                gcloud storage cp --recursive {gcs_bucket_path} ~/dbt_projects
            '''
        )
        if run_status == 0:
            return {
                'deployment_status': 'success',
                'deployed_path': f'~/dbt_projects/{project_name}'
            }
        else:
            return {
                'deployment_status': f'error - {run_status}',
                'deployed_path': None
            }
    except Exception as err:
        return {
            'deployment_status': f'error - {str(err)}',
            'deployed_path': None
        }        
