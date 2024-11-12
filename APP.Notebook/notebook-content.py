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

# # Sample App
# 
# **You will need access to an embedding API and a chat model API, here Azure OpenAI is used**
# 
# ChromaDB is presisted at the end of the ETL pipeline
# 
# SQLite file however does not presist stable in storage
# 
# Since the sqlite loading process is quite a bit faster than chroma loading, sqlite is loaded from silver layer table in this same notebook.
# 
# *Authors: Jani Kuhno, Joel Kataja*

# MARKDOWN ********************

# ### Setup

# CELL ********************

from langchain.sql_database import SQLDatabase
import pandas as pd
from sqlalchemy import create_engine
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings

from langchain_openai import AzureChatOpenAI
from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough

from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_core.messages import SystemMessage
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Workspace-dependent abfss path to the "silver_games_stats" table 
lakehouse_table_path = "abfss://CS2_test@onelake.dfs.fabric.microsoft.com/CsHouse.Lakehouse/Tables/silver_games_stats"  # Adjust this path as needed

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

# MARKDOWN ********************

# ### SQLite db
# 
# From the silver layer table create a SQLite db to use with a LangChain default sql agent.
# 
# *Note: since the creation takes only about 20s, it can be done alongside initializing the sample app. Presisting the .db file in a lakehouse causes difficulties.*

# CELL ********************

df = spark.read.format("delta").load(lakehouse_table_path)

# Convert the Spark DataFrame to a Pandas DataFrame for easier SQLite handling
pandas_df = df.toPandas()

# Step 2: Save the DataFrame to a SQLite .db file
db_path = "/lakehouse/default/Files/lakehouse_data.db"  # Path to save the .db file

engine = create_engine(f"sqlite:///{db_path}")
table_name = "csTable"  # Name for the table in SQLite
pandas_df.to_sql(table_name, con=engine, if_exists="replace", index=False)


# Step 3: Connect to the SQLite database with LangChain
db = SQLDatabase.from_uri(f"sqlite:///{db_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Verify
db.run("SELECT * FROM csTable LIMIT 100")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Initiate Chroma
# 
# *Chroma vectorstore can be presisted in a lakehouse, data is loaded in the transcripts pipeline.*
# 
# Here the vectorstore is initialized from presist direcotory and prepared to be used as a retriever.

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

retriever = vector_store.as_retriever(
    search_type="mmr", search_kwargs={"k": 5, "fetch_k": 5}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### SQL Agent
# 
# A pre-configured langgraph agent with sql query tools.
# 
# Rate limiter is set to avoid getting a rate limit error, the TPM limit is easily breached with long sql queries.

# CELL ********************

from langchain_core.rate_limiters import InMemoryRateLimiter

rate_limiter = InMemoryRateLimiter(
    requests_per_second=0.016,  # <-- Can only make a request once every minute
     check_every_n_seconds=1,  # Wake up every 1s to check whether allowed to make a request,
     max_bucket_size=1,  # maximum burst size.
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql_llm = AzureChatOpenAI(
    model="gpt-4-turbo-2024-04-09",
    #model="gpt-4o",
    max_tokens=500,
    azure_endpoint=AZURE_ENDPOINT,
    api_version=API_VERSION,
    openai_api_key=OPENAI_API_KEY,
    temperature=0,
    rate_limiter=rate_limiter
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

toolkit = SQLDatabaseToolkit(db=db, llm=sql_llm)

tools = toolkit.get_tools()

# Since the single table to query is known in this scenario, ListSQLDatabaseTool can be omitted
tools.pop(2)

# Default langchain sql agent system message
SQL_PREFIX = """You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct SQLite query to run, then look at the results of the query and return the answer.
Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.
You can order the results by a relevant column to return the most interesting examples in the database.
Never query for all the columns from a specific table, only ask for the relevant columns given the question.
You have access to tools for interacting with the database.
Only use the below tools. Only use the information returned by the below tools to construct your final answer.
You MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.

DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.

The table to query is called 'csTable'. You start by querying the schema of the table."""

system_message = SystemMessage(content=SQL_PREFIX)

agent_executor = create_react_agent(sql_llm, tools, messages_modifier=system_message)

agent_executor.get_graph()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get answer to a question using the sql agent
# Inputs: user question from the UI
# Returns1: the final answer of the agent graph
# Returns2: if used, return the sql query from the last tool call
def gen_sql_answer(question):
    events_list = list(agent_executor.stream(
        {"messages": [("user", question)]},
        stream_mode="values",
    ))

    try:
        query = events_list[-1]["messages"][-3].tool_calls[0]["args"]["query"]
    except IndexError:
        query = "Did not generate a query"
    except:
        query = "Did not generate a query"   

    # return the answer plus db query info
    return events_list[-1]["messages"][-1].content, query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### RAG
# 
# Retrieval-augmented generation assistant.
# 
# Used to reduce the context length for the final answer assistant, acts as a middle man to find answers from youtube transcripts of pro match VOD's.
# 
# *Note: a new chat model is defined without rate limiting. The length of context for requests is set by the text splitting + retrieval configs, and does not exceed rate limits.*
# 
# *This saves a minute in answering time of the final answer assistant*

# CELL ********************

rag_llm = AzureChatOpenAI(
    model="gpt-4-turbo-2024-04-09",
    max_tokens=500,
    azure_endpoint=AZURE_ENDPOINT,
    api_version=API_VERSION,
    openai_api_key=OPENAI_API_KEY,
    temperature=0,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rag_prompt = PromptTemplate(
    template=""" 
        You are an assistant, answering questions about Counter-Strike 2.
        To support your question answering, you are provided a transcript from the casters of a match and podcasts about hot topics in the Counter Strike 2 scene.
        In the transcripts, some player names are not spelled correctly. Use the player name described in the question in your answer.

        Here is the question: {question}
        Here are the transcripts: {documents}
    """,
    input_variables=["question", "documents"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get answer to a question using the RAG assistant
# Inputs: user question from the UI
# Returns1: RAG answer
# Returns2: document splits used as context in the prompt
def gen_rag_answer(question):
    context = retriever.invoke(question)

    invoker = {"question": question,
               "documents": context,  
              }
    
    rag_chain = rag_prompt | rag_llm | StrOutputParser()

    rag_answer = rag_chain.invoke(invoker)
    #print(rag_answer)

    return rag_answer, context


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Summary assistant
# 
# A simple langchain chat assistant to provide answer to a user question.
# 
# Makes the final answer by evaluating and combining interim answers of sql agent and RAG assistant.

# CELL ********************

# again a separate chat model, for ease of experimentation
summary_llm = AzureChatOpenAI(
    model="gpt-4-turbo-2024-04-09",
    #model="gpt-4o",
    max_tokens=500,
    azure_endpoint=AZURE_ENDPOINT,
    api_version=API_VERSION,
    openai_api_key=OPENAI_API_KEY,
    temperature=0,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

summary_prompt = PromptTemplate(
    template=""" 
        You are an assistant assessing answers to a Counter Strike 2 related question and tasked with providing a final answer.
        You are provided with an answer generated from pro match highlights video transcripts by retrieval augmentation AI and
        an answer generated from pro statistics database by an sql q&a AI.

        Assess which of the two answers better answer the original question, and formulate a final answer based on that. If both answers answer the 
        original question well, use both. Use your knowledge to correct misspellings of pro names, for example if the question mentions 'Donk' (which is the correct spelling), the generated answers may mention 'Don'. This corrects to 'Donk'.
        This can happen with other player names too, remember that the correct spelling is the one in the question. Do not mention the incorrect spelling in the final answer.

        Give your answer as a concise final answer to the original question, do not include the assessing process in the answer. Just hint the source or sources.


        Here is the answer by retrieval augmentation AI: {rag}
        Here is the answer by an sql q&a AI: {sql}
        Here is the original question: {question}
    """,
    input_variables=["rag", "sql", "question"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get answer to a question
# Inputs: user question from the UI
# Returns1: final answer
# Returns2: RAG answer, for debugging
# Returns3: RAG context docs, for debugging
# Returns4: sql agent answer, for debugging
# Returns5: sql agent sql query, for debugging
def gen_final_answer(question):
    rag_answer, context = gen_rag_answer(question)
    sql_answer, query = gen_sql_answer(question)

    summary_invoker = {"rag": rag_answer,
                       "sql": sql_answer,
                       "question": question,
          }

    summary_chain = summary_prompt | summary_llm | StrOutputParser()

    final_answer = summary_chain.invoke(summary_invoker)
    return final_answer, rag_answer, context, sql_answer, query

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### UI
# 
# Gradio UI to act as a sample app ui to demonstrate the solution.
# 
# Use the link provided in **Running on public URL:** in the cell outputs to access the UI.

# CELL ********************

import gradio as gr

my_theme = gr.Theme.from_hub("earneleh/paris")

#Modify theme to align with brand colors
theme = my_theme.set(
    button_primary_background_fill="#FC7B00", #orange
    button_primary_background_fill_hover="#F89A42", #dim orange
    body_background_fill="#FAF8F4", #off white
    body_text_color="#136A61", # green
    input_background_fill_focus="#FAF8F4" #off white, too dark in the theme
)

# UI
with gr.Blocks(theme=theme) as iface:
    gr.Markdown("This app answers your **CS2** questions based on a pro match database and transcripts of pro play VOD's. *Due to rate limits, takes around 5 mins per answer if database is queried*")
    gr.Markdown("If you get an error when asking a question immediately after another, wait 1 minute. This is most likely due to rate limits. ")
    inp = gr.Textbox(label="Input question", placeholder="Ask your question here")
    btn = gr.Button("Run")
    out = gr.Textbox(label="Answer")

    with gr.Row():
        with gr.Accordion(label="Additional info", open=False):
            rag_answer = gr.Textbox(label="RAG answer")
            context = gr.Textbox(label="RAG context documents")
            sql_answer = gr.Textbox(label="SQL answer")
            sql_query = gr.Textbox(label="SQL query")

    btn.click(fn=gen_final_answer, inputs=inp, outputs=[out, rag_answer, context, sql_answer, sql_query])

# Launch the Gradio app with share=True
iface.launch(share=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#ans = gen_final_answer("Whats the opinion on Shiro")
#ans

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
