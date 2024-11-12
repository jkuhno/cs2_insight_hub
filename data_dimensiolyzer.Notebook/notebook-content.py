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

# # Notebook for gold layer loading
# Makes the transformations in order to load silver layer table "silver_game_stats" to dimension and fact tables.
# 
# Might not be necessary for the SQL agent, but enables efficient use of Power BI and conforms to the medallion architecture.
# 
# *Authors: Jani Kuhno*

# CELL ********************

df = spark.read.table("silver_games_stats")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import*
from pyspark.sql.functions import col, lit, to_date, dayofmonth, month, year, date_format

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dimension: Date

# CELL ********************


    
# Define the schema for the dimdate_gold table
DeltaTable.createIfNotExists(spark) \
    .tableName("dimdate_gold") \
    .addColumn("last_update", DateType()) \
    .addColumn("Day", IntegerType()) \
    .addColumn("Month", IntegerType()) \
    .addColumn("Year", IntegerType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lit, to_date, dayofmonth, month, year, date_format

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfdimDate_gold = df.select(to_date(col("last_update")).alias("last_update"), \
        dayofmonth("last_update").alias("Day"), \
        month("last_update").alias("Month"), \
        year("last_update").alias("Year"), \
    ).orderBy("last_update").dropDuplicates(["last_update"])
# Display the first 10 rows of the dataframe to preview your data
display(dfdimDate_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/dimdate_gold')
    
dfUpdates = dfdimDate_gold
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.last_update = updates.last_update'
  ) \
   .whenMatchedUpdate(set =
    {
         
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "last_update": "updates.last_update",
      "Day": "updates.Day",
      "Month": "updates.Month",
      "Year": "updates.Year",
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dimension: teams

# CELL ********************


DeltaTable.createIfNotExists(spark) \
    .tableName("dimteam_gold") \
    .addColumn("team_id", IntegerType()) \
    .addColumn("team_name",  StringType()) \
    .addColumn("player1", LongType()) \
    .addColumn("player2", LongType()) \
    .addColumn("player3", LongType()) \
    .addColumn("player4", LongType()) \
    .addColumn("player5", LongType()) \
    .addColumn("last_update", DateType()) \
    .addColumn("is_current_roster", BooleanType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfdimTeam_gold_team1 = df.select(col("team_1_id"), \
        col("team_1_name"), \
        col("team1player1id"),  \
        col("team1player2id"),  \
        col("team1player3id"),  \
        col("team1player4id"),  \
        col("team1player5id"),  \
        to_date(col("last_update")).alias("last_update"), \
        #col("is_current_roster")
    ).orderBy("last_update").dropDuplicates(["team_1_id"]).withColumn("is_current_roster", lit(False))
# Display the first 10 rows of the dataframe to preview your data
display(dfdimTeam_gold_team1.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dfdimTeam_gold_team2 = df.select(col("team_2_id"), \
        col("team_2_name"), \
        col("team2player1id"),  \
        col("team2player2id"),  \
        col("team2player3id"),  \
        col("team2player4id"),  \
        col("team2player5id"),  \
        to_date(col("last_update")).alias("last_update"), \
        #col("is_current_roster")
    ).orderBy("last_update").dropDuplicates(["team_2_id"]).withColumn("is_current_roster", lit(False))
# Display the first 10 rows of the dataframe to preview your data
display(dfdimTeam_gold_team2.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/dimteam_gold')
    
dfUpdates = dfdimTeam_gold_team1
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    """
    gold.team_id = updates.team_1_id AND 
    gold.team_name = updates.team_1_name AND 
    gold.player1 = updates.team1player1id AND 
    gold.player2 = updates.team1player2id AND
    gold.player3 = updates.team1player3id AND 
    gold.player4 = updates.team1player4id AND 
    gold.player5 = updates.team1player5id AND
    gold.last_update = updates.last_update
    """
  ) \
   .whenMatchedUpdate(set =
    {
         
    }
  ) \
 .whenNotMatchedInsert(values =
    {
    "team_id": "updates.team_1_id",
    "team_name": "updates.team_1_name",
    "player1": "updates.team1player1id", 
    "player2": "updates.team1player2id",
    "player3": "updates.team1player3id",
    "player4": "updates.team1player4id", 
    "player5": "updates.team1player5id",
    "is_current_roster": "updates.is_current_roster",
    "last_update": "updates.last_update",

    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/dimteam_gold')
    
dfUpdates = dfdimTeam_gold_team2
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    """
    gold.team_id = updates.team_2_id AND 
    gold.team_name = updates.team_2_name AND 
    gold.player1 = updates.team2player1id AND 
    gold.player2 = updates.team2player2id AND
    gold.player3 = updates.team2player3id AND 
    gold.player4 = updates.team2player4id AND 
    gold.player5 = updates.team2player5id AND
    gold.last_update = updates.last_update
    """
  ) \
   .whenMatchedUpdate(set =
    {
         
    }
  ) \
 .whenNotMatchedInsert(values =
    {
    "team_id": "updates.team_2_id",
    "team_name": "updates.team_2_name",
    "player1": "updates.team2player1id", 
    "player2": "updates.team2player2id",
    "player3": "updates.team2player3id",
    "player4": "updates.team2player4id", 
    "player5": "updates.team2player5id",
    "is_current_roster": "updates.is_current_roster",
    "last_update": "updates.last_update",

    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dimension: players

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("dimplayer_gold") \
    .addColumn("player_id", LongType()) \
    .addColumn("player_name",  StringType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

player_columns = [
    ("team1player1id", "team1player1name"),
    ("team1player2id", "team1player2name"),
    ("team1player3id", "team1player3name"),
    ("team1player4id", "team1player4name"),
    ("team1player5id", "team1player5name"),
    ("team2player1id", "team2player1name"),
    ("team2player2id", "team2player2name"),
    ("team2player3id", "team2player3name"),
    ("team2player4id", "team2player4name"),
    ("team2player5id", "team2player5name"),
]

# Create a DataFrame for each pair and union them
player_dfs = [
    df.select(col(player_id).cast(LongType()).alias("player_id"), col(player_name).alias("player_name"))
    for player_id, player_name in player_columns
]

# Union all DataFrames and select distinct entries
unique_players_df = player_dfs[0]
for player_df in player_dfs[1:]:
    unique_players_df = unique_players_df.union(player_df)

unique_players_df = unique_players_df.distinct().orderBy("player_name")
unique_players_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/dimplayer_gold')
    
dfUpdates = unique_players_df
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.player_id = updates.player_id AND gold.player_name = updates.player_name'
  ) \
   .whenMatchedUpdate(set =
    {
         
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "player_id": "updates.player_id",
      "player_name": "updates.player_name",
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Facts: factStats

# CELL ********************

DeltaTable.createIfNotExists(spark) \
    .tableName("factStats_gold") \
    .addColumn("game_id", StringType()) \
    .addColumn("series_id",  IntegerType()) \
    .addColumn("last_update", DateType()) \
    .addColumn("team_1_id", IntegerType()) \
    .addColumn("team_1_score", IntegerType()) \
    .addColumn("team_1_won", BooleanType()) \
    .addColumn("team_2_id", IntegerType()) \
    .addColumn("team_2_score", IntegerType()) \
    .addColumn("team_2_won", BooleanType()) \
    .addColumn("team1player1id", LongType()) \
    .addColumn("team1player1kills", IntegerType()) \
    .addColumn("team1player1deaths", IntegerType()) \
    .addColumn("team1player1plant", IntegerType()) \
    .addColumn("team1player2id", LongType()) \
    .addColumn("team1player2kills", IntegerType()) \
    .addColumn("team1player2deaths", IntegerType()) \
    .addColumn("team1player2plant", IntegerType()) \
    .addColumn("team1player3id", LongType()) \
    .addColumn("team1player3kills", IntegerType()) \
    .addColumn("team1player3deaths", IntegerType()) \
    .addColumn("team1player3plant", IntegerType()) \
    .addColumn("team1player4id", LongType()) \
    .addColumn("team1player4kills", IntegerType()) \
    .addColumn("team1player4deaths", IntegerType()) \
    .addColumn("team1player4plant", IntegerType()) \
    .addColumn("team1player5id", LongType()) \
    .addColumn("team1player5kills", IntegerType()) \
    .addColumn("team1player5deaths", IntegerType()) \
    .addColumn("team1player5plant", IntegerType()) \
    .addColumn("team2player1id", LongType()) \
    .addColumn("team2player1kills", IntegerType()) \
    .addColumn("team2player1deaths", IntegerType()) \
    .addColumn("team2player1plant", IntegerType()) \
    .addColumn("team2player2id", LongType()) \
    .addColumn("team2player2kills", IntegerType()) \
    .addColumn("team2player2deaths", IntegerType()) \
    .addColumn("team2player2plant", IntegerType()) \
    .addColumn("team2player3id", LongType()) \
    .addColumn("team2player3kills", IntegerType()) \
    .addColumn("team2player3deaths", IntegerType()) \
    .addColumn("team2player3plant", IntegerType()) \
    .addColumn("team2player4id", LongType()) \
    .addColumn("team2player4kills", IntegerType()) \
    .addColumn("team2player4deaths", IntegerType()) \
    .addColumn("team2player4plant", IntegerType()) \
    .addColumn("team2player5id", LongType()) \
    .addColumn("team2player5kills", IntegerType()) \
    .addColumn("team2player5deaths", IntegerType()) \
    .addColumn("team2player5plant", IntegerType()) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = df.withColumn("last_update", to_date(col("last_update")).alias("last_update")) 
# Create Sales_gold dataframe 
dffactStats_gold = df.drop("finished", 
                  "team_1_name", 
                  "team_2_name", 
                  "team1player1name",
                  "team1player2name",
                  "team1player3name",
                  "team1player4name",
                  "team1player5name",
                  "team2player1name",
                  "team2player2name",
                  "team2player3name",
                  "team2player4name",
                  "team2player5name",)
# Display the first 10 rows of the dataframe to preview your data
display(dffactStats_gold.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTable = DeltaTable.forPath(spark, 'Tables/factstats_gold')
    
dfUpdates = dffactStats_gold

columns = dfUpdates.columns  # List of column names in the updates DataFrame
values = {col_name: col(f"updates.{col_name}") for col_name in columns}
    
deltaTable.alias('gold') \
  .merge(
    dfUpdates.alias('updates'),
    'gold.game_id = updates.game_id'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values = values
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
