# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6bf3ae1d-83fa-4bfa-916d-b68e8dae4662",
# META       "default_lakehouse_name": "CsHouse",
# META       "default_lakehouse_workspace_id": "2f4df65b-a3e2-423f-be79-386e691d4213"
# META     },
# META     "environment": {
# META       "environmentId": "537ae0e5-05c3-4d9b-8a29-9d1847646510",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Transcripts to Chroma - Notebook
# This notebook adds already downloaded transcripts from the "/lakehouse/default/Files/transcripts/" folder and adds them to Chroma.
# 
# **Requirements**
# 
# - The notebook "youtube_transcript_api" is required to run before this.
# - You need access to an embedding API. Azure Open AI proxy is used here.

# CELL ********************

# Azure OpenAi proxy variables
AZURE_ENDPOINT = "your-endpoint"
OPENAI_API_KEY = "your-api-key"
API_VERSION = "2023-09-01-preview" # your api version

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import time

from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings
from langchain_core.documents import Document

from langchain_text_splitters import RecursiveCharacterTextSplitter

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the path to the main transcripts folder
folder_path = '/lakehouse/default/Files/transcripts/'

# Initialize an empty list to hold the document contents
documents = []

# Loop through each subfolder and file in the main folder
for root, _, files in os.walk(folder_path):
    for filename in files:
        file_path = os.path.join(root, filename)
        
        # Check if the current item is a file
        if os.path.isfile(file_path):
            # Open the file and read its contents
            with open(file_path, 'r', encoding='utf-8') as file:
                text = file.read()

            # Split the text into chunks using RecursiveCharacterTextSplitter
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=100)
            splits = text_splitter.split_text(text)

            # Create Document objects from the splits and add them to the document list
            for chunk in splits:
                documents.append(Document(page_content=chunk))

# Print the document list to verify
print(documents)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

embed = AzureOpenAIEmbeddings(
    model="text-embedding-ada-002",
    azure_endpoint=AZURE_ENDPOINT,
    openai_api_key=OPENAI_API_KEY,
    #openai_api_version="2024-06-01-preview"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vector_store = Chroma(
    collection_name="text_collection",
    embedding_function=embed,
    persist_directory="/lakehouse/default/Files/chroma_langchain_db_text", 
)

# For every run, reset collection before populating with documents in the next cell, to avoid making duplicates
vector_store.reset_collection()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Adds document in small batches to avoid rate-limit error.
def add_documents_with_timestep(documents):
    extend_list = []
    chunk_size = 1100
    for i in range(0, len(documents), chunk_size):
        print("adding documents..")
        vector_store.add_documents(documents[i:i + chunk_size])
        extend_list.extend(documents[i:i + chunk_size])
        print("sleeping...")
        time.sleep(60)
    return extend_list

add_documents_with_timestep(documents)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
