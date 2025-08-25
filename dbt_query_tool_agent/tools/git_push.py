import subprocess
import os
import base64
from google.adk.tools import FunctionTool

def git_push(repo_path: str, branch_name: str, commit_message: str) -> dict:
    """
    Performs a git add, commit, and push operation to a specified branch
    in a local Git repository.

    Args:
        repo_path (str): The local path to the Git repository.
        branch_name (str): The name of the branch to push to.
        commit_message (str): The commit message for the changes.

    Returns:
        dict: A dictionary indicating the success or failure of the operation
              and any relevant output or error messages.
    """
    if not os.path.isdir(os.path.join(repo_path, '.git')):
        return {"status": "ERROR", "message": f"'{repo_path}' is not a valid Git repository."}

    try:
        # Change to the repository directory
        os.chdir(repo_path)

        # Add all changes
        add_result = subprocess.run(['git', 'add', '.'], capture_output=True, text=True, check=True)
        print(f"Git Add Output:\n{add_result.stdout}")

        # Commit changes
        commit_result = subprocess.run(['git', 'commit', '-m', commit_message], capture_output=True, text=True, check=True)
        print(f"Git Commit Output:\n{commit_result.stdout}")

        # Get PAT from environment variable
        pat = os.getenv('GIT_PAT')
        if not pat:
            return {"status": "ERROR", "message": "GIT_PAT environment variable not set."}

        # Encode PAT for basic authentication
        encoded_pat = base64.b64encode(f"user:{pat}".encode()).decode()

        # Push to the specified branch with PAT authentication
        # This assumes the remote URL is already set up, and we're just adding auth for the push
        # For GitHub, the URL format would typically be https://github.com/OWNER/REPO.git
        # We'll modify the remote URL temporarily for the push operation to include the PAT
        # This is a simplified approach; a more robust solution might involve git credential helper
        subprocess.run(['git', 'config', 'http.extraheader', f'Authorization: Basic {encoded_pat}'], check=True)
        # Push to the specified branch
        push_result = subprocess.run(['git', 'push', 'origin', branch_name], capture_output=True, text=True, check=True)
        print(f"Git Push Output:\n{push_result.stdout}")
        return {
            "status": "SUCCESS",
            "message": f"Successfully pushed to branch '{branch_name}'.",
            "add_output": add_result.stdout,
            "commit_output": commit_result.stdout,
            "push_output": push_result.stdout
        }

    except subprocess.CalledProcessError as e:
        return {
            "status": "FAILED",
            "message": f"Git command failed: {e.stderr}"
        }