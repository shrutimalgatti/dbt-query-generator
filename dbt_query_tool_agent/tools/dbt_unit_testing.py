import os
import subprocess
from google.adk.tools import FunctionTool
from google.cloud import storage
from urllib.parse import urlparse
import shutil # Added for robust directory cleanup

# Import dbt's programmatic invocation tools
try:
    from dbt.cli.main import dbtRunner
    from dbt.events.functions import setup_event_logger, reset_logger
    from dbt.events.base_types import EventMsg
except ImportError:
    # Handle cases where dbt is not installed in the execution environment
    dbtRunner = None
    setup_event_logger = None
    reset_logger = None
    EventMsg = None
    print("Warning: dbt-core or dbt-bigquery not found. Programmatic invocation will not work.")


# Initialize Google Cloud Storage client
STORAGE_CLIENT = storage.Client()

# Custom logger to capture dbt output
class CaptureLogOutput:
    def __init__(self):
        self.stdout_lines = []
        self.stderr_lines = []

    def handle(self, event: EventMsg):
        # This is a basic handler. DBT events can be complex.
        # For simplicity, we'll try to capture text output.
        if hasattr(event, 'info'):
            # Some events might have an 'info' attribute with a message
            self.stdout_lines.append(str(event.info))
        else:
            # Fallback for other event types, might not be perfect
            self.stdout_lines.append(str(event)) # Capture event representation

    def get_stdout(self):
        return "\n".join(self.stdout_lines)

    def get_stderr(self):
        # dbt events usually don't separate stdout/stderr cleanly in EventMsg
        # For programmatic invocation, errors are usually in exceptions or specific event types.
        # We'll just return any captured stdout for now, or specifically log exceptions if any.
        return ""


def run_dbt_project(dbt_project_gcs_path: str, dbt_command: str) -> dict:
    """
    Runs specified dbt commands (e.g., 'run', 'test') for a dbt project
    stored in a Google Cloud Storage (GCS) bucket using dbt's programmatic invocation API.

    Args:
        dbt_project_gcs_path (str): The GCS URL to the dbt project folder
                                     (e.g., 'gs://your-bucket/your-dbt-project-name').
        dbt_command (str): The dbt command to execute ('run' or 'test').

    Returns:
        dict: A dictionary indicating the success or failure of the dbt command
              and any relevant output or error messages.
    """
    if not dbtRunner:
        return {"result": "ERROR", "message": "dbt-core is not installed, programmatic invocation is not possible."}

    if not dbt_project_gcs_path.startswith('gs://'):
        return {"result": "ERROR", "message": "Invalid GCS project path. Must start with 'gs://'."}
    if dbt_command not in ['run', 'test']:
        return {"result": "ERROR", "message": "Unsupported dbt command. Only 'run' and 'test' are supported."}

    temp_dir = None # Initialize to None for cleanup in finally block
    try:
        parsed_url = urlparse(dbt_project_gcs_path)
        bucket_name = parsed_url.netloc
        project_gcs_prefix = parsed_url.path.lstrip('/')

        # Create a temporary directory to download the dbt project
        temp_dir = f"/tmp/dbt_project_{os.urandom(4).hex()}"
        os.makedirs(temp_dir, exist_ok=True)
        print(f"Created temporary directory: {temp_dir}")

        bucket = STORAGE_CLIENT.bucket(bucket_name)

        # List all blobs under the project prefix and download them
        blobs = bucket.list_blobs(prefix=project_gcs_prefix)
        downloaded_files_count = 0
        for blob in blobs:
            # Construct local file path, preserving directory structure
            relative_path = os.path.relpath(blob.name, project_gcs_prefix)
            local_file_path = os.path.join(temp_dir, relative_path)

            # Ensure the directory structure exists locally
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            print(f"Downloading {blob.name} to {local_file_path}")
            blob.download_to_filename(local_file_path)
            downloaded_files_count += 1

        if downloaded_files_count == 0:
            return {"result": "ERROR", "message": f"No dbt project files found at {dbt_project_gcs_path}"}

        # Programmatic dbt invocation
        # Reset logger to ensure clean state and avoid interference from previous runs
        reset_logger()
        log_capture = CaptureLogOutput()
        # Set up dbt's event logger to capture output
        setup_event_logger(log_capture.handle)

        # Instantiate dbtRunner
        dbt = dbtRunner()

        # Build the CLI arguments
        cli_args = [dbt_command, "--project-dir", temp_dir]

        print(f"Executing dbt command programmatically: dbt {' '.join(cli_args)}")

        # Execute the dbt command
        result = dbt.invoke(cli_args)

        stdout_output = log_capture.get_stdout()
        stderr_output = log_capture.get_stderr() # Will be empty or very limited as errors are usually exceptions

        if result.exception:
            # If an exception occurred during dbt invocation
            return {
                "result": "FAILED",
                "command": ' '.join(cli_args),
                "stdout": stdout_output,
                "stderr": str(result.exception), # Capture the exception as stderr
                "message": f"DBT command '{dbt_command}' failed with an exception: {result.exception}"
            }
        elif result.success:
            return {
                "result": "SUCCESS",
                "command": ' '.join(cli_args),
                "stdout": stdout_output,
                "stderr": stderr_output,
                "message": f"DBT command '{dbt_command}' executed successfully."
            }
        else:
            # If dbt command ran but reported non-success (e.g., tests failed)
            return {
                "result": "FAILED",
                "command": ' '.join(cli_args),
                "stdout": stdout_output,
                "stderr": stderr_output,
                "message": f"DBT command '{dbt_command}' failed."
            }

    except Exception as e:
        return {"result": "ERROR", "message": f"An unexpected error occurred during dbt invocation: {str(e)}"}
    finally:
        # Clean up temporary directory
        if temp_dir and os.path.exists(temp_dir):
            print(f"Cleaning up temporary directory: {temp_dir}")
            try:
                shutil.rmtree(temp_dir)
            except Exception as cleanup_e:
                print(f"Error during cleanup of {temp_dir}: {cleanup_e}")

# Create the FunctionTool instance
run_dbt_project_tool = FunctionTool(run_dbt_project)