import io
import os
import json
from typing import List, Dict # Kept for internal type clarity
from urllib.parse import urlparse
import pandas as pd
from google.cloud import storage
from google.adk.tools import FunctionTool
from dbt_query_tool_agent.utils import infer_dbt_project_name_from_gcs_path

def generate_dbt_test_report(
    test_plan_gcs_path: str,
    test_results: str
) -> dict:
    """
    Generates a test report by merging dbt test results with the original test plan.

    Args:
        test_plan_gcs_path (str): The GCS URL of the test plan CSV file.
        test_results (str): A JSON string representing a list of dictionaries from the
                            test runner tool. Each dict should have keys: 'test_name',
                            'status', 'message'.

    Returns:
        dict: A dictionary containing the GCS path of the generated test report,
              which can be used for downloading.
    """
    try:
        storage_client = storage.Client()
        
        # Parse the JSON string into a Python object
        try:
            test_results_list = json.loads(test_results)
        except json.JSONDecodeError:
            return {'error': 'Invalid JSON format for test_results.'}

        # Download the original test plan
        parsed_url = urlparse(test_plan_gcs_path)
        bucket_name = parsed_url.netloc
        blob_name = parsed_url.path.lstrip('/')
        
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        if not blob.exists():
            return {'error': f'Test plan not found at: {test_plan_gcs_path}'}
            
        test_plan_df = pd.read_csv(io.BytesIO(blob.download_as_bytes()))
        
        # Create a dictionary from the test results for easy lookup
        results_map = {result['test_name']: result for result in test_results_list}
        
        # Add new columns for results
        test_plan_df['Status'] = 'NOT RUN'
        test_plan_df['Failure Reason'] = ''
        
        # Populate results based on the 'Test ID' matching the test name
        for index, row in test_plan_df.iterrows():
            test_id = str(row['Test ID'])
            if test_id in results_map:
                result = results_map[test_id]
                test_plan_df.at[index, 'Status'] = result['status']
                if result['status'] != 'PASS':
                    test_plan_df.at[index, 'Failure Reason'] = result.get('message', '')

        # Save the report to a new CSV file in GCS
        report_content = test_plan_df.to_csv(index=False)
        
        dbt_project_name = infer_dbt_project_name_from_gcs_path(test_plan_gcs_path)
        report_gcs_path = f"{dbt_project_name}/test_reports/{dbt_project_name}_test_report.csv"
        report_blob = bucket.blob(report_gcs_path)
        
        report_blob.upload_from_string(report_content, content_type='text/csv')
        
        return {
            'result': 'SUCCESS',
            'downloadable_gcs_path': f'gs://{bucket_name}/{report_gcs_path}'
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {'result': 'ERROR', 'message': str(e)}

generate_dbt_test_report_tool = FunctionTool(generate_dbt_test_report)