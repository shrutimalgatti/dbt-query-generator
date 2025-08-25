import os
import subprocess
import re
from google.adk.tools import FunctionTool
from google.cloud import storage
from urllib.parse import urlparse
import shutil
import sys
from io import StringIO
from typing import Optional
from dbt.cli.main import dbtRunner

def run_unit_testing_dbt_project(dbt_project_gcs_path: str, dbt_command: str, model_name: Optional[str] = None) -> dict:
    """
    Runs specified dbt commands (e.g., 'run', 'test') for a dbt project
    stored in a Google Cloud Storage (GCS) bucket using dbt's programmatic invocation API.
    Can also run a specific model within the project.

    Args:
        dbt_project_gcs_path (str): The GCS URL to the dbt project folder
                                     (e.g., 'gs://your-bucket/your-dbt-project-name').
        dbt_command (str): The dbt command to execute ('run' or 'test').
        model_name (Optional[str]): The name of a specific model to run. If None, all models
                                     or tests within the project (based on dbt_command) are executed.

    Returns:
        dict: A dictionary indicating the success or failure of the dbt command
              and any relevant output or error messages.
    """
    if not dbtRunner:
        return {"result": "ERROR", "message": "dbt-core is not installed, programmatic invocation is not possible."}

    if not dbt_project_gcs_path.startswith('gs://'):
        return {"result": "ERROR", "message": "Invalid GCS project path. Must start with 'gs://'."}
    if dbt_command not in ['run', 'test', 'snapshot', 'ls']:
        return {"result": "ERROR", "message": "Unsupported dbt command. Only 'run', 'test', and 'snapshot' are supported."}

    storage_client = storage.Client()
    if model_name and dbt_command == 'test':
        print(f"Warning: model_name specified for 'test' command. Running 'dbt test --select {model_name}'.")

    temp_dir = None
    try:
        parsed_url = urlparse(dbt_project_gcs_path)
        bucket_name = parsed_url.netloc
        project_gcs_prefix = parsed_url.path.lstrip('/')

        # Create a temporary directory to download the dbt project
        # This will be the root of our dbt project.
        temp_dir = f"/tmp/dbt_project_{os.urandom(4).hex()}"
        os.makedirs(temp_dir, exist_ok=True)
        print(f"Created temporary directory: {temp_dir}")

        bucket = storage_client.bucket(bucket_name)

        # List all blobs under the project prefix and download them
        blobs = bucket.list_blobs(prefix=project_gcs_prefix)
        downloaded_files_count = 0
        for blob in blobs:
            # Construct local file path, preserving directory structure
            # The key change is here: the project root is 'temp_dir' not 'temp_dir/dbt'
            relative_path = os.path.relpath(blob.name, project_gcs_prefix)
            local_file_path = os.path.join(temp_dir, relative_path)

            # Ensure the directory structure exists locally
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            print(f"Downloading {blob.name} to {local_file_path}")
            blob.download_to_filename(local_file_path)
            downloaded_files_count += 1

        if downloaded_files_count == 0:
            return {"result": "ERROR", "message": f"No dbt project files found at {dbt_project_gcs_path}"}

        # Verify profiles.yml exists
        profiles_yml_path = os.path.join(temp_dir, "profiles.yml")
        if not os.path.exists(profiles_yml_path):
            return {
                "result": "ERROR",
                "message": (f"Error: 'profiles.yml' not found in the downloaded dbt project at {temp_dir}. "
                            "Please ensure your GCS dbt project includes a profiles.yml file at its root.")
            }
        print(f"Found profiles.yml at: {profiles_yml_path}")

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        sys.stdout = captured_stdout = StringIO()
        sys.stderr = captured_stderr = StringIO()
        
        result = None
        try:
            dbt = dbtRunner()
            cli_args = [dbt_command, "--project-dir", temp_dir, "--profiles-dir", temp_dir]
            if model_name:
                cli_args.extend(["--select", model_name])

            print(f"Executing dbt command programmatically: dbt {' '.join(cli_args)}")

            result = dbt.invoke(cli_args)
        finally:
            stdout_val = captured_stdout.getvalue()
            stderr_val = captured_stderr.getvalue()
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            print(f"--- Captured dbt stdout ---\n{stdout_val}\n--- End dbt stdout ---")
            if stderr_val:
                print(f"--- Captured dbt stderr ---\n{stderr_val}\n--- End dbt stderr ---")

        full_log = stdout_val
        if stderr_val:
            full_log += "\n--- STDERR ---\n" + stderr_val

        # Special handling for 'dbt test' to provide structured output
        if dbt_command == 'test':
            test_results_list = []
            if hasattr(result, 'results') and result.results:
                for res in result.results:
                    # Start with the default message from the result object
                    failure_message = res.message or ""
                    # If the test failed, try to find a more descriptive message in the logs.
                    if str(res.status) == 'fail':
                        # A regex to find the detailed failure reason for a specific test.
                        # It looks for "Failure in test test_name" and captures the next line.
                        failure_pattern = re.compile(rf"Failure in test {res.node.name}.*?\n\s*(.*)")
                        match = failure_pattern.search(full_log)
                        if match:
                            # The captured group is the more descriptive failure reason.
                            failure_message = match.group(1).strip()
                    test_results_list.append({
                        "test_name": res.node.name,
                        "status": str(res.status).upper(),
                        "message": failure_message
                    })
            
            summary_line = ""
            for line in reversed(full_log.splitlines()):
                if line.strip().startswith("Done. PASS="):
                    summary_line = line.strip()
                    break

            response = {
                "result": "SUCCESS" if result.success else "FAILED",
                "command": ' '.join(cli_args),
                "message": f"dbt test completed. {summary_line}",
                "test_results": test_results_list
            }

            # If the test command failed, include the full log for debugging.
            if not result.success:
                response['stdout'] = full_log

            return response

        # Existing logic for other commands (run, snapshot, ls)
        if result.success:
            message = f"DBT command '{dbt_command}' executed successfully."
            return {
                "result": "SUCCESS",
                "command": ' '.join(cli_args),
                "message": message
            }

        if result.exception:
            return {
                "result": "FAILED",
                "command": ' '.join(cli_args),
                "stdout": full_log,
                "stderr": str(result.exception),
                "message": f"DBT command '{dbt_command}' failed with an exception. See output for details."
            }
        else:  # not result.success
            return {
                "result": "FAILED",
                "command": ' '.join(cli_args),
                "stdout": full_log,
                "message": f"DBT command '{dbt_command}' failed. See output for details."
            }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"result": "ERROR", "message": f"An unexpected error occurred during dbt invocation: {str(e)}"}
    finally:
        if temp_dir and os.path.exists(temp_dir):
            print(f"Cleaning up temporary directory: {temp_dir}")
            try:
                shutil.rmtree(temp_dir)
            except Exception as cleanup_e:
                print(f"Error during cleanup of {temp_dir}: {cleanup_e}")

run_unit_testing_dbt_project_tool = FunctionTool(run_unit_testing_dbt_project)