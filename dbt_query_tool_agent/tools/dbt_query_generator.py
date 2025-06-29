import uuid
from google.api_core.exceptions import GoogleAPIError
from datetime import datetime, timezone
from google.cloud import storage
from google.cloud import bigquery
import vertexai
from urllib.parse import urlparse
from vertexai.generative_models import GenerativeModel, GenerationConfig,Part
from google.adk.tools import FunctionTool
from dotenv import load_dotenv
import os
load_dotenv()
import time
import traceback

import pandas as pd
from typing import Dict, Any, List



def generate_dbt_query(gcs_file_path: str) -> Dict[str, Any]:
    """
    Reads a CSV/Excel file's raw content from a Google Cloud Storage (GCS) bucket,
    and then uses the Gemini model to parse this raw content and generate
    BigQuery DBT SQL queries.

    Args:
        gcs_file_path (str): The GCS URI of the uploaded CSV or Excel file
                             (e.g., "gs://your-bucket-name/path/to/sttm.csv").

    Returns:
        Dict[str, Any]: A dictionary containing the generated DBT SQL or an error message.
    """
    
    model = GenerativeModel("gemini-2.5-pro")
    
    try: 
        # 1. Parse the GCS URI
        parsed_uri = urlparse(gcs_file_path)
        bucket_name = parsed_uri.netloc
        blob_name = parsed_uri.path.lstrip('/')
        

        # 2. Initialize the GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        

        # 3. Download the file content
        file_content_bytes = blob.download_as_bytes()
        
        

        # 4. Decode the file content
        try:
            file_content = file_content_bytes.decode('utf-8')
            
        except UnicodeDecodeError:
            # If it's a binary Excel, provide a generic message or hint LLM
            file_content = f"Binary file content (likely Excel). Gemini, interpret this as best as you can or indicate if a specific Excel parser is needed: {file_content_bytes[:500]}..."  # Provide a snippet

        if not file_content.strip():
            return {"status": "error", "message": "The uploaded file is empty or contains no readable data."}

        # Construct a detailed prompt for Gemini
        prompt_parts = [
            "You are an expert BigQuery DBT data engineer. Your task is to generate accurate BigQuery DBT SQL models.",
            "Below is the raw text content of a Source-to-Target Mapping (STTM) file.",
            "This file can be either a CSV or an Excel spreadsheet (represented as text).",
            "You MUST carefully read and parse the content of this STTM to understand the mapping rules.",
            "Pay close attention to the following columns:",
            "  - 'Target table': The full name of the target BigQuery table (e.g., `project.dataset.table_name`). This often indicates the DBT model name.",
            "  - 'Target Column': The name of the column in the target table.",
            "  - 'Target Datatype': The desired BigQuery data type for the target column.",
            "  - 'Source Table': The full name of the source BigQuery table. Note that `(T1)` indicates an alias for the primary source table.",
            "  - 'Source Column': The name of the column in the source table.",
            "  - 'Transformation Logic / Derivation Rule': The SQL expression or logic to derive the target column.",
            "  - 'Join Table': Additional tables to join with the primary source.",
            "  - 'Join Key': The join condition.",
            "",
            "**Crucial Parsing Instructions for you (Gemini):**",
            "1.  **Row Association**: 'Target table' and 'Source Table' might only be populated on the first row for a given logical model. Assume subsequent rows without these values belong to the same model until a new value appears.",
            "2.  **Transformation Logic Fallback**: If 'Transformation Logic / Derivation Rule' is empty for a 'Target Column', assume a direct mapping from 'Source Column'. If 'Source Column' is also empty, assume the target column name itself is the source (e.g., `target_column_name` AS `target_column_name`).",
            "3.  **Multi-line SQL**: Be aware that 'Transformation Logic / Derivation Rule' often contains multi-line SQL expressions (e.g., `CASE WHEN`). Interpret these correctly.",
            "4.  **Join Interpretation**: Extract all relevant join information ('Join Table', 'Join Key') for each target model. Consolidate unique join conditions that apply to a given target table.",
            "5. **Source Aliases**: Pay close attention to aliases like `(T1)` in 'Source Table'. If 'Join Key' refers to aliases like `T3`, ensure you use these aliases consistently for joined tables.",
            "",
            "**DBT SQL Generation Instructions:**",
            "1.  **Model Structure**: For each identified 'Target table', generate a complete and valid BigQuery DBT SQL model.",
            "2.  **File Naming Convention**: The model should conceptually be saved in `models/{target_model_name.replace('shruti-test-414408.test_lbg.', '').replace('.', '_')}.sql`. (e.g., `onsfdp.sql` for `shruti-test-414408.test_lbg.onsfdp`).",
            "3.  **CTE (`source_data`)**: Start with a Common Table Expression (CTE) named `source_data`. This CTE should select all necessary columns from the primary source table and any joined tables.",
            "4.  **Source Referencing**: Use DBT's `{{{{ source('project_id_or_dataset', 'table_name') }}}}` macro for raw source tables (like `shruti-test-414408.test_lbg.onspd_full`). If a table is a DBT model, use `{{{{ ref('model_name') }}}}`. Based on the STTM, assume all full table names like `shruti-test-414408.test_lbg.ITL125` are raw sources for now.",
            "5.  **Joins in CTE**: Include `LEFT JOIN` clauses within the `source_data` CTE based on the 'Join Table' and 'Join Key' information. Assign aliases (T1, T2, T3, etc.) as indicated in the STTM or consistently if not explicitly given.",
            "6.  **Final `SELECT` Statement**: In the main `SELECT` statement after the CTE, apply the 'Transformation Logic / Derivation Rule' for each 'Target Column'.",
            "7.  **Aliasing Target Columns**: Alias each transformed expression to its `Target Column` name.",
            "8.  **BigQuery Syntax**: Ensure all SQL functions and syntax are compatible with BigQuery.",
            "9.  **Escape Backslashes**: If transformation logic contains escaped backslashes (e.g., `\\n`), ensure they are correctly represented in the generated SQL (e.g., `\\n` remains `\\n` for BigQuery string literals if it's meant to be a literal newline).",
            "10. **Output Format**: Provide **only** the SQL code within a single markdown code block (`sql`). Do not include any additional text, comments, or explanations outside this block. If multiple DBT models are generated from the STTM, provide each in a separate, clearly labeled SQL markdown block.",
            "11. **FOR slowly changding dimensions(SCD2) Logics use dbt snapsots "
            "",
            "**Raw STTM Content to Parse:**",
            "```text",
            file_content,
            "```",
            "\nGenerate the BigQuery DBT SQL model(s) now based on the above STTM content."
        ]

        full_prompt = "\n".join(prompt_parts)

        
        generation_config = GenerationConfig(temperature=0.1, max_output_tokens=4096) 
        #response = model.generate_content(
         #   contents=[{"role": "user", "parts": [Part.from_text(full_prompt)]}],
          #  generation_config=generation_config
        #)
        response = model.generate_content(full_prompt)

        generated_sqls_output = {}
        if response.candidates:
            generated_sql = response.candidates[0].content.parts[0].text
            generated_sqls_output["main_model"] = generated_sql
        else:
            generated_sqls_output["main_model"] = "Could not generate SQL: No candidates found."

        if generated_sqls_output:
            return {
                "status": "success",
                "generated_sqls": generated_sqls_output,
                "message": "DBT SQL models generated successfully by Gemini."
            }
        else:
            return {
                "status": "error",
                "message": "No DBT SQL models could be generated."
            }

    except Exception as e:
        traceback.print_exc()  # Print full traceback for debugging
        return {"status": "error", "message": f"Error processing file or generating SQL: {str(e)}. Please ensure the file is valid and the GCS path is correct."}

    
        
#Wrap python function as function tool
generate_dbt_query_tool = FunctionTool(generate_dbt_query)