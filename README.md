# CS2 Insight Hub


## Demo video
Watch our demo video here: https://youtu.be/Pu0DlIR3rKk

## How we built it
The project is divided into data ingestion pipelines, lakehouse storage and a question answering AI assistant. The two data pipelines ingest unstructured and structured data.

We also provide a sample dataset, in order to test the solution without having access to grid.gg API, where the structured data comes from. (https://grid.gg/)

Unstructured data comes in a form of Youtube transcripts from pro match highlights's and CS2 podcasts. Structured data is queried from grid.gg API through GraphQL, and then transformed according to the medallion architecture, using PySpark notebooks.

After the data is loaded into a lakehouse, unstructured data is vectorized into a Chroma vectorstore to act as a basis for a RAG assistant, and structured data is loaded as an SQLite database to act as a basis for SQL agent.

The AI assitant consists of three models: An SQL agent, a RAG assistant and a summarizing assistant. The SQL agent is a default langgraph react agent, the other two are basic langchain chains.

To use the assistant, a Gradio UI is launched in the notebook and also accessible through a public URL.

## Prerequisites
- Existing Microsoft Fabric environment
- Microsoft Fabric license or Microsoft Fabric trial capacity (https://learn.microsoft.com/en-us/fabric/get-started/fabric-trial)
- More info about how to set up a free Microsoft Fabric environment: https://www.youtube.com/watch?v=6K4ADjkWjaY
- Microsoft Fabric Administrator role, or GitHub sync enabled for the tenant
  
## Setup with sample data
1. Fork this repository
2. Clone the repo or download as zip (We need the "sample_data" folder on local disk, use your preferred method)
3. From the top-right corner, go to -> Settings -> Developer Settings -> Personal Access Tokens -> Tokens (Classic) -> Generate new token (classic)
   - Note: CS2 Insight Hub
   - Checkmark "Repo"
   - leave everything else default and click "Generate token"
   - copy the result
4. Create a new workspace in Fabric, configure the way you like but use Trial or Premium license
5. If GitHub sync not enabled
   - In Fabric, access Admin portal from the top-right cog icon
   - "Filter by keyword" box type git, select "Users can sync workspace items with GitHub repositories" and enable, Apply. (Configure the settings the way you like)
6. In your workspace, go to Workspace settings -> Git integration -> GitHub -> Add account
   - name, what you like
   - Personal Access Token generated earlier
   - fork URL
   - Add
7. Connect (Branch can give you an error, wait a bit and it should go away.) -> Connect & Sync
8. After the sync is complete, go to your workspace and click "CS2-env" -> top right corner "Publish" -> Publish all
9. Go to CsHouse Lakehouse
   - Next to the "Files" in Explorer, click three dots -> Upload -> Upload Files -> navigate to the sample_files folder and upload the PARQUET file
   - Refresh
   - Click three dots next to the file name of the uploaded file -> Load to Tables -> New table -> name it "silver_games_stats" -> Load
   - Next to the "Files" in Explorer, click three dots -> Upload -> Upload Folder -> navigate to the sample_files folder and upload the "transcripts" folder
10. While still in the Lakehouse, from the top ribbon click Open notebook -> Existing notebook -> APP -> Open
11. From the explorer -> Lakehouses -> Tables, click three dots next to "silver_games_stats" -> Copy path
12. Paste the path as the value for "lakehouse_table_path" in the second code cell of the notebook, leave "data_source" as it is
13. In the third code cell, input the Azure endpoint variables
14. From the top ribbon, click Run all
15. Scroll to the bottom of the notebook and click the link "Running on public URL" in the UI cell outputs
16. Ask away! Note: the sample data only covers a month or so, and some tournaments and teams are missing like NaVi and G2 due to limitations in GRID Open Access program.


#### OPTIONAL: Data pipeline setup

<blockquote>
<details>
<summary>
Additional optional setup for GRID API and youtube-transcript-api
</summary>
    
### Additional prerequisites
- GRID Open Access account and a GRID API key (apply here: https://grid.gg/open-access-application-form/)
- A proxy service like Web Share or similar. Can be omitted if prepared to run a python script

### Setup
1. From sample data setup, follow the steps 1, 3, 4, 5, 6, 7, 8.
2. In the workspace, go to the "TranscriptsNotebooks" folder and open youtube-transcripts-api. Follow the instructions in the notebook.
3. In the same workspace folder, open transcripts_chroma, update the Azure OpenAI variables
4. Close the notebooks and run "transcript-pipeline" pipeline.
5. Open the "grid-pipeline", click on the Notebook "GRID_API_query"
   - go to Settings -> Base parameters
   - set the GRID_API_KEY value as the GRID Open Access key
   - set the default date value as the YYYY-MM-DD date you want to start querying the stats. 2024-01-01 runs for about 30-45 minutes.
6. Go to CsHouse, from the top ribbon click Open notebook -> Existing notebook -> APP -> Open
11. From the explorer -> Lakehouses -> Tables, click three dots next to "silver_games_stats" -> Copy path
12. Paste the path as the value for "lakehouse_table_path" in the second code cell of the notebook, set "data_source" as "pipeline"


</details>
</blockquote>


