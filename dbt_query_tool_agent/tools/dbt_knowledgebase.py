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

import os
from google.cloud import discoveryengine_v1beta as discoveryengine
from langchain_google_community import VertexAISearchRetriever
from google.api_core.client_options import ClientOptions

from google.protobuf.json_format import MessageToDict
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple

# --- Configuration ---
# IMPORTANT: Replace these placeholders with your actual Google Cloud project details.
# You can find these in your Google Cloud Console.

# Your Google Cloud Project ID
PROJECT_ID = "shruti-test-414408"

# The location where your data store is created (e.g., "global", "us-central1").
# Ensure this matches the region of your data store.
LOCATION_ID = "global"

# The ID of your unstructured data store.
# You can find this by navigating to 'AI Applications' -> 'Data Stores' in the
# Google Cloud Console and clicking on your data store's name.
SEARCH_APP_ID = "projects/{self.project_id}/locations/{self.location_id}/collections/default_collection/engines/dbt_1752484781698/servingConfigs/default_config"

# --- Authentication ---
# Ensure your environment is authenticated to Google Cloud.
# The recommended way for local development is to run:
# `gcloud auth application-default login`
# in your terminal.
# Alternatively, you can set the GOOGLE_APPLICATION_CREDENTIALS environment variable
# to the path of your service account key JSON file:
# os.environ = "/path/to/your/service-account-key.json"

vertexai.init(project="shruti-test-414408", location="us-central1")

def retrieve_and_print_pdf_content(query_text: str):
    """
    Retrieves content from a Vertex AI Search unstructured data store
    based on a query and prints the extracted content (extractive segments).

    Args:
        query_text (str): The natural language query to search for within your
                          ingested PDF documents.
    """
    print(f"Attempting to retrieve content for query: '{query_text}' from search app: {SEARCH_APP_ID}")

    try:
        project_id="shruti-test-414408"
        LOCATION_ID="global"
        data_store_id="dbt_1752479115203"
        # Initialize the VertexAISearchRetriever.
        # We configure it to return 'extractive segments', which are verbatim text passages
        # from the documents that are most relevant to the query.[1]
        # Setting 'get_extractive_answers=False' ensures we get segments instead of brief answers.
        # 'max_extractive_segment_count=1' is the current typical return limit per document.[1]
        retriever = VertexAISearchRetriever(
            project_id=PROJECT_ID,
            location_id=LOCATION_ID,
            data_store_id=data_store_id, # <--- Use search_engine_id here
            # DO NOT include data_store_id if you are using search_engine_id
            get_extractive_answers=False,
            max_extractive_segment_count=1
        )

        # Perform the search query.
        # The 'invoke' method sends the query to your Vertex AI Search data store
        # and returns a list of 'Document' objects.
        documents = retriever.invoke(query_text)

        if not documents:
            print("No relevant documents or content found for the query.")
            return

        print("\n--- Extracted Content from PDF Documents ---")
        # Iterate through the retrieved documents and print their extracted content.
        for i, doc in enumerate(documents):
            print(f"\nDocument {i+1}:")
            # The 'page_content' field contains the extracted text (extractive segment).[1]
            print(f"Content:\n{doc.page_content}")
            # The 'metadata' field can contain additional information about the document,
            # such as its source URI, if it was included during ingestion.[1]
            if doc.metadata:
                print(f"Metadata: {doc.metadata}")
            print("-" * 50)
            return doc.page_content

    except Exception as e:
        print(f"An error occurred: {e}")
        print("\nTroubleshooting Tips:")
        print(f"- Double-check that 'PROJECT_ID', 'LOCATION_ID', and 'SEARCH_APP_ID' are correctly set.")
        print(f"- Ensure the 'Discovery Engine API' is enabled for your Google Cloud project.")
        print(f"- Verify that the service account used for authentication has the 'Discovery Engine Viewer' IAM role (or equivalent permissions like 'discoveryengine.servingConfigs.search') on your project or data store.[2, 3]")
        print(f"- Confirm that PDF files have been successfully ingested and indexed into your data store. Indexing can take time.[4, 5]")
        print(f"- Ensure the search app connected to your data store has Enterprise edition features enabled, as extractive segments require it.[6, 7]")
        print(f"- Try a query that you know is present in your ingested PDF content.")
