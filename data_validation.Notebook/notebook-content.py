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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Notebook for data validation and cleaning
# Inspect the bronze level data, make minor modifications, remove missing and duplicate rows.
# 
# Results in a silver level delta table
# 
# *Authors: Jani Kuhno*

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Read data

# CELL ********************

# Read the bronze table and re-arrange field names
df = spark.read.format("delta").load("abfss://CS2@onelake.dfs.fabric.microsoft.com/CsHouse.Lakehouse/Tables/bronze_games_stats")
df = df.withColumn("last_update", col("last_update").cast(TimestampType()))
df = df.orderBy(col("last_update").desc())
df = df.select("game_id",
            "series_id",
            "finished",
            "last_update",
            "team_1_id",
            "team_1_name",
            "team_1_score",
            "team_1_won",
            "team_2_id",
            "team_2_name",
            "team_2_score",
            "team_2_won",
            "team1player1id",
            "team1player1name",
            "team1player1kills",
            "team1player1deaths",
            "team1player1plant",
            "team1player2id",
            "team1player2name",
            "team1player2kills",
            "team1player2deaths",
            "team1player2plant",
            "team1player3id",
            "team1player3name",
            "team1player3kills",
            "team1player3deaths",
            "team1player3plant",
            "team1player4id",
            "team1player4name",
            "team1player4kills",
            "team1player4deaths",
            "team1player4plant",
            "team1player5id",
            "team1player5name",
            "team1player5kills",
            "team1player5deaths",
            "team1player5plant",
            "team2player1id",
            "team2player1name",
            "team2player1kills",
            "team2player1deaths",
            "team2player1plant",
            "team2player2id",
            "team2player2name",
            "team2player2kills",
            "team2player2deaths",
            "team2player2plant",
            "team2player3id",
            "team2player3name",
            "team2player3kills",
            "team2player3deaths",
            "team2player3plant",
            "team2player4id",
            "team2player4name",
            "team2player4kills",
            "team2player4deaths",
            "team2player4plant",
            "team2player5id",
            "team2player5name",
            "team2player5kills",
            "team2player5deaths",
            "team2player5plant",
            )

            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Clean data
# 1. Drop unfinished matches and duplicates
# 2. Drop rows where all players have zero kills
# 3. Drop rows where neither of the teams have accumulated at least 13 round wins (score)
#    
#    *In MR12, a win in regulation means one team will get 13 rounds. Since it is very rare to have tournaments where games can end up in a draw, we treat games without a MR12 regulation winner as anomalies*


# CELL ********************

# Before dropping duplicate rows, drop unfinished games in order to not preserve same game with two different sets of values
df = df.filter(col("finished") == True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop duplicates
df = df.drop_duplicates()
df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check if there are rows where all players have zero kills, most likely that game is either cancelled or data is corrupted
kill_columns = [
    "team1player1kills",
    "team1player2kills",
    "team1player3kills",
    "team1player4kills",
    "team1player5kills",
    "team2player1kills",
    "team2player2kills",
    "team2player3kills",
    "team2player4kills",
    "team2player5kills",
]

# Create a condition which counts each kill column and checks if the sum is 0 (since no negative values are expected, this indicates that all values were zero)
all_kills_zero_condition = sum(F.col(col) for col in kill_columns) == 0

# Filter out rows where all 10 kill columns are 0
# Inverse filter discards rows that meet the condition
filtered_df = df.filter(~all_kills_zero_condition)

filtered_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Define the condition to keep rows where at least one score is 13 or more
at_least_one_score_13_condition = (F.col("team_1_score") >= 13) | (F.col("team_2_score") >= 13)

# Filter DataFrame to discard rows where both scores are 12 or less
filtered_df = filtered_df.filter(at_least_one_score_13_condition)

filtered_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Save the cleaned data as silver layer table

# CELL ********************

filtered_df.write.format("delta").mode("overwrite").saveAsTable("silver_games_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
