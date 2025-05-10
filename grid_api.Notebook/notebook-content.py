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
# META       "default_lakehouse_workspace_id": "2f4df65b-a3e2-423f-be79-386e691d4213",
# META       "known_lakehouses": [
# META         {
# META           "id": "6bf3ae1d-83fa-4bfa-916d-b68e8dae4662"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "537ae0e5-05c3-4d9b-8a29-9d1847646510",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook for accessing GRID API
# Visit https://grid.gg/open-access/ to get an API key.
# 
# **Flows like this:**
# 
# 1. Get all CS2 series ids (identifying unique face-offs between teams, consisting of one or multiple games)
# 2. Query basic player stats per series relating to the series ids
# 3. Untangle the results into a list of low-dimensional dicts (tabular format)
# 4. Load the results into a spark dataframe and write the dataframe into a Delta table in the lakehouse
# 
# *Authors: Jani Kuhno, Joel Kataja*
# 


# MARKDOWN ********************

# 
# ### Setup

# PARAMETERS CELL ********************

### PARAMETERS ###

# Acquired from GRID
GRID_API_KEY = " "

# If a source-of-truth table is not found, start querying from this date
# Wont be used if a populated table is found, eg. This notebook has run successfully before
default_date = "2024-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

from datetime import datetime, timezone, timedelta
import time
import isodate

from tqdm.notebook import tqdm

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### GraphQL query functions

# CELL ********************

# GraphQL requests client maker
# Get url from https://portal.grid.gg/documentation/graphql-playground (requires a GRID account for access)

def transport_client(url: str):
    transport = AIOHTTPTransport(url=url,
                                 headers= {
                                     "x-api-key": GRID_API_KEY
                                 }
                                 )
    client = Client(transport=transport, fetch_schema_from_transport=True)
    return client

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Time now in a format supported by GraphQL server on GRID side (ISO)
def get_time_now():
    time_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")
    time_now = time_now[:-2] + ":" + time_now[-2:]
    return time_now

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if there is data already and start querying games after the last one
# Overlap of one hour is included to account for the pipeline run time
# If no games recorded, start from the start of the year

if spark._jsparkSession.catalog().tableExists("silver_games_stats"):
    delta_table_df = spark.table("bronze_games_stats")
    last_game_record = delta_table_df.select("last_update").first()
    last_game_record = last_game_record["last_update"]
    date_obj = datetime.strptime(last_game_record, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    new_date_obj = date_obj + timedelta(hours=-1)
    last_game_record = new_date_obj.strftime("%Y-%m-%dT%H:%M:%S%z")
    last_game_record = last_game_record[:-2] + ":" + last_game_record[-2:]

else:
    last_game_record = f"{default_date}T00:00:00+02:00"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get all series id's

# CELL ********************

# GraphQL query to get all CS2 series, year-to-date
# Server allows for max 50 results per page, 20 pages per minute. A while loop starts a new page after the last result of previous page
# and run until result item "hasNextPage" turns to False to indicate the end of results
# every 19 pages there is a minute-long timeout in order to not exceed allowed rate
query = gql("""
    query GetAllSeries($cursor: Cursor, $time_last: String, $time_now: String) {
     allSeries(
      after: $cursor
      first: 50
      filter:{
       titleId: 28
       startTimeScheduled:{
        gte: $time_last
        lte: $time_now
        }
       types: ESPORTS  # Official matches only
      }
      orderBy: StartTimeScheduled
      orderDirection: DESC
    ) {
     totalCount,
     pageInfo{
      hasPreviousPage
      hasNextPage
      startCursor
      endCursor
     }
     edges{
      cursor
      node{
        id
        startTimeScheduled
      }
     }
    }
    }
    """
)
# Initialize variables for the loop
cursor = None
last_time = last_game_record
has_next_page = True
all_data = []
iteration_count = 1
client = transport_client("https://api-op.grid.gg/central-data/graphql")

# Loop through the pages
while has_next_page:
     # Define variables with the current cursor for the query
    params = {"cursor": cursor,
              "time_last": last_time,
              "time_now": get_time_now()}
    
     # Make the request
    
    result = await client.execute_async(query, variable_values=params)
    # Extract relevant data
    page_info = result["allSeries"]["pageInfo"]
    edges = result["allSeries"]["edges"]

    # Append data from this page to the all_data list
    all_data.extend(edges)

    if result["allSeries"]["totalCount"] >= 50:       
        # Update cursor and has_next_page for the next iteration
        has_next_page = page_info["hasNextPage"]
        cursor = page_info["endCursor"]
        
        # timeout to limit requests to < 20 per minute
        iteration_count += 1
        if iteration_count % 20 == 0:
           print(f"Pausing for 1 minute, after {iteration_count} requests and {iteration_count * 50} samples..")
           time.sleep(60)
           iteration_count += 1
    else:
        print("Queried resulted only one page")
        break

# Display all retrieved data
print("All data hits:", len(all_data))

# Create a list of series id's
series_list = []
for i in all_data:
    sample = i["node"]["id"]
    series_list.append(sample)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

series_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get series stats based on series ids
# **Note:** runs for around 30 minutes

# CELL ********************

# GraphQL query to extract stats from series
# A singular result includes nested games of the series in question (for example, 3 games in a sweeped best-of-5)
# Can result in missing values, and can result in ongoing series as the data feed is live
stats_query = gql("""
    query GetLiveSeriesState($id: ID!){
    seriesState(id: $id) {
    id
    valid
    updatedAt
    format
    started
    finished
    teams {
      name
      won
    }
    games {
      id
      # startedAt
      # duration TransportQueryError: {'message': 'Requested field is only available from version 3.16'
      sequenceNumber
      teams {
        id
        name
        score
        won
        players {
          id
          name
          kills
          deaths
          objectives {
            type
            completionCount
          }
        }
      }
    }
  }
}
"""
)
stats = []
series_stats_client = transport_client("https://api-op.grid.gg/live-data-feed/series-state/graphql")
for i in tqdm(series_list, desc="Querying series data"):
    params = {"id": i}
    series_stats = await series_stats_client.execute_async(stats_query, variable_values=params)
    stats.append(series_stats)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

len(stats)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### De-nest the GraphQL responses into a tabular format (list of dicts)

# CELL ********************

# Series stats nest the games stats, which nest the teams stats, which nest player stats. Parse the dimensionality into a tabular format
# Completely missing series are handled in the call, here is handling for missing players,
# skip games with less players than five-a-side
def get_games(series_stats):
    games_list = []
    for game in series_stats['seriesState']["games"]:
        if len(game["teams"][0]["players"]) < 5 or len(game["teams"][1]["players"]) < 5:
            continue
        #last_update = datetime.strptime(game["startedAt"], '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
        #last_update = last_update + isodate.parse_duration(game["duration"])
        #last_update = last_update.strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'

        game_row = {"game_id": game["id"],
            "series_id": series_stats['seriesState']["id"],
            "finished": series_stats['seriesState']["finished"],
            "last_update": series_stats['seriesState']["updatedAt"],
            "team_1_id": game["teams"][0]["id"],
            "team_1_name": game["teams"][0]["name"],
            "team_1_score": game["teams"][0]["score"],
            "team_1_won": game["teams"][0]["won"],
            "team1player1id": game["teams"][0]["players"][0]["id"],
            "team1player1name": game["teams"][0]["players"][0]["name"],
            "team1player1kills": game["teams"][0]["players"][0]["kills"],
            "team1player1deaths": game["teams"][0]["players"][0]["deaths"],
            "team1player1plant": next((item['completionCount'] for item in game["teams"][0]["players"][0]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team1player2id": game["teams"][0]["players"][1]["id"],
            "team1player2name": game["teams"][0]["players"][1]["name"],
            "team1player2kills": game["teams"][0]["players"][1]["kills"],
            "team1player2deaths": game["teams"][0]["players"][1]["deaths"],
            "team1player2plant": next((item['completionCount'] for item in game["teams"][0]["players"][1]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team1player3id": game["teams"][0]["players"][2]["id"],
            "team1player3name": game["teams"][0]["players"][2]["name"],
            "team1player3kills": game["teams"][0]["players"][2]["kills"],
            "team1player3deaths": game["teams"][0]["players"][2]["deaths"],
            "team1player3plant": next((item['completionCount'] for item in game["teams"][0]["players"][2]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team1player4id": game["teams"][0]["players"][3]["id"],
            "team1player4name": game["teams"][0]["players"][3]["name"],
            "team1player4kills": game["teams"][0]["players"][3]["kills"],
            "team1player4deaths": game["teams"][0]["players"][3]["deaths"],
            "team1player4plant": next((item['completionCount'] for item in game["teams"][0]["players"][3]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team1player5id": game["teams"][0]["players"][4]["id"],
            "team1player5name": game["teams"][0]["players"][4]["name"],
            "team1player5kills": game["teams"][0]["players"][4]["kills"],
            "team1player5deaths": game["teams"][0]["players"][4]["deaths"],
            "team1player5plant": next((item['completionCount'] for item in game["teams"][0]["players"][4]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team_2_id": game["teams"][1]["id"],
            "team_2_name": game["teams"][1]["name"],
            "team_2_score": game["teams"][1]["score"],
            "team_2_won": game["teams"][1]["won"],
            "team2player1id": game["teams"][0]["players"][0]["id"],
            "team2player1name": game["teams"][0]["players"][0]["name"],
            "team2player1kills": game["teams"][0]["players"][0]["kills"],
            "team2player1deaths": game["teams"][0]["players"][0]["deaths"],
            "team2player1plant": next((item['completionCount'] for item in game["teams"][0]["players"][0]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team2player2id": game["teams"][0]["players"][1]["id"],
            "team2player2name": game["teams"][0]["players"][1]["name"],
            "team2player2kills": game["teams"][0]["players"][1]["kills"],
            "team2player2deaths": game["teams"][0]["players"][1]["deaths"],
            "team2player2plant": next((item['completionCount'] for item in game["teams"][0]["players"][1]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team2player3id": game["teams"][0]["players"][2]["id"],
            "team2player3name": game["teams"][0]["players"][2]["name"],
            "team2player3kills": game["teams"][0]["players"][2]["kills"],
            "team2player3deaths": game["teams"][0]["players"][2]["deaths"],
            "team2player3plant": next((item['completionCount'] for item in game["teams"][0]["players"][2]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team2player4id": game["teams"][0]["players"][3]["id"],
            "team2player4name": game["teams"][0]["players"][3]["name"],
            "team2player4kills": game["teams"][0]["players"][3]["kills"],
            "team2player4deaths": game["teams"][0]["players"][3]["deaths"],
            "team2player4plant": next((item['completionCount'] for item in game["teams"][0]["players"][3]["objectives"] if item['type'] == 'plantBomb'), 0),
            "team2player5id": game["teams"][0]["players"][4]["id"],
            "team2player5name": game["teams"][0]["players"][4]["name"],
            "team2player5kills": game["teams"][0]["players"][4]["kills"],
            "team2player5deaths": game["teams"][0]["players"][4]["deaths"],
            "team2player5plant": next((item['completionCount'] for item in game["teams"][0]["players"][4]["objectives"] if item['type'] == 'plantBomb'), 0),
            }
        games_list.append(game_row)
    return games_list

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Get all CS2 ESPORTS stats

# CELL ********************

# Skip missing entries and call formatting function for each entry
# Resulting data rows are identified by game id, and contain the series id for face-off grouping
rows = []
for i in stats:
    if not i:
        continue
    games = get_games(i)
    rows.extend(games)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Load the data into a spark DataFrame
# Then write the data into a Delta table in the lakehouse.

# CELL ********************

df = spark.createDataFrame(rows)
df = df.orderBy(col("last_update").desc())

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").mode("append").saveAsTable("bronze_games_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 
