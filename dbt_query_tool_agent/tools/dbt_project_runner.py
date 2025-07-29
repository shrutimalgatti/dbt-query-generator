import subprocess
import vertexai
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel, GenerationConfig
from google.adk.tools import FunctionTool


def run_dbt_project(dbt_project_path: str) -> dict:
    try:
        dbt_debug_status = subprocess.run(f'dbt debug --project-dir {dbt_project_path} --profiles-dir {dbt_project_path}', capture_output = True, text = True, check = True, shell = True)
        dbt_run_status = subprocess.run(f'dbt run --project-dir {dbt_project_path}  --profiles-dir {dbt_project_path}', capture_output = True, text = True, check = True, shell = True)

        return {
            'status': 'success',
            'return_code': dbt_run_status.returncode,
            'output': dbt_run_status.stdout,
        }
    except subprocess.CalledProcessError as cmd_err:
        return {
            'status': 'error',
            'return_code': cmd_err.returncode,
            'output': cmd_err.stderr
        }
    except Exception as err:
        return {
            'status': 'failure',
            'return_code': None,
            'output': str(err)
        }    
run_dbt_project_tool = FunctionTool(run_dbt_project)