import os
import getpass
from urllib.parse import urlparse
from typing import Dict,Optional

USER_AGENT = "GitHub-Downloader-ADK/2.0"

def _create_github_headers(token: str = "") -> Dict[str, str]:
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': USER_AGENT
    }
    if token:
        headers['Authorization'] = f'Bearer {token}'
    return headers

def _get_auth_token(token: str = "") -> str:
    token = token or os.getenv("GITHUB_TOKEN")
    if not token or not token.strip():
        token = getpass.getpass("Enter your GitHub Personal Access Token: ")
    return token.strip()

def _parse_repo_path(repository: str) -> tuple[Optional[str], Optional[str]]:
    if repository.startswith(('http://', 'https://')):
        repo_path = repository.split('github.com/')[-1].rstrip('.git')
    else:
        repo_path = repository
    if '/' not in repo_path:
        return None, None
    return repo_path.split('/', 1)

def infer_dbt_project_name_from_gcs_path(gcs_path: str) -> str:
    """
    Infers the dbt project name from a GCS path.

    It handles two cases:
    1. The initial STTM upload from Gradio, which has a path like
       'gradio_uploads/.../{uuid}-{original_filename}'. It extracts
       the 'original_filename' and returns its stem.
    2. An internally generated artifact path, like 'project_name/dbt/tests/file.sql'.
       It extracts the 'project_name' from the beginning of the path.

    Args:
        gcs_path (str): The full GCS path (e.g., 'gs://bucket/path/to/file.csv').

    Returns:
        str: The inferred dbt project name.
    """
    if not gcs_path:
        return ""
    
    blob_name = urlparse(gcs_path).path.lstrip('/')
    
    if 'gradio_uploads/' in blob_name:
        full_filename = os.path.basename(blob_name)
        parts = full_filename.split('-', 1)
        original_filename = parts[1] if len(parts) > 1 else full_filename
        return os.path.splitext(original_filename)[0]
    else:
        path_parts = blob_name.split('/')
        return path_parts[0] if path_parts else ""