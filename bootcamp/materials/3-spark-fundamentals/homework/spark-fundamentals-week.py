from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, sum, count, desc, min, countDistinct


def explicitly_broadcast_join_on_maps(matches, maps):
    """Explicitly join on maps"""
    # Explicitly broadcast JOINs maps
    return matches.join(broadcast(maps), matches.mapid == maps.mapid)

def explicitly_broadcast_join_on_medals(medals_matches_players, medals):
    """Explicitly join on medals"""

    # Explicitly broadcast JOINs medals
    return medals_matches_players.join(broadcast(medals), medals.medal_id == medals_matches_players.medal_id)

def create_matches_bucketed(spark, matches):
    """Create a table matches_bucketed which is bucket by matchId into 16 buckets"""
    # DDL Matches bucket table
    spark.sql("""DROP TABLE IF EXISTS homework.matches_bucketed""")
    bucketedDDL = """
    CREATE TABLE IF NOT EXISTS homework.matches_bucketed (
        match_id STRING,
        mapid STRING,
        is_team_game BOOLEAN,
        playlist_id STRING,
        completion_date TIMESTAMP
    )
    USING iceberg
    CLUSTERED BY (match_id) INTO 16 buckets;
    """
    spark.sql(bucketedDDL)

    # Save matches data to table matches_bucketed
    matches.select("match_id", "mapid", "is_team_game", "playlist_id", "completion_date") \
        .write.mode("append") \
        .bucketBy(16, "match_id") \
        .saveAsTable("homework.matches_bucketed")

def create_match_details_bucketed(spark, match_details):
    """Create a table create_match_details_bucketed which is bucket by matchId into 16 buckets"""
    spark.sql("""DROP TABLE IF EXISTS homework.match_details_bucketed""")
    match_details_bucketed_ddl = """
    CREATE TABLE IF NOT EXISTS homework.match_details_bucketed (
        match_id STRING,
        player_gamertag STRING,
        team_id STRING,
        player_total_kills INTEGER,
        player_total_deaths INTEGER,
        did_win INTEGER
    )
    USING iceberg
    CLUSTERED BY (match_id) INTO 16 buckets;
    """
    spark.sql(match_details_bucketed_ddl)

    # Save match_details_bucketed table
    match_details.select("match_id", "player_gamertag", "team_id", "player_total_kills", "player_total_deaths", "did_win") \
        .write.mode("append") \
        .bucketBy(16, "match_id") \
        .saveAsTable("homework.match_details_bucketed")

def create_medals_matches_players_bucketed(spark, medals_matches_players):
    """Create a table medals_matches_players_bucketed which is bucket by matchId into 16 buckets"""
    # DDL - medals_matches_players_bucketed table
    spark.sql("""DROP TABLE IF EXISTS homework.medals_matches_players_bucketed""")
    medals_matches_players_bucketed_ddl = """
    CREATE TABLE IF NOT EXISTS homework.medals_matches_players_bucketed (
        match_id STRING,
        player_gamertag STRING,
        medal_id STRING,
        count STRING
    )
    USING iceberg
    CLUSTERED BY (match_id) INTO 16 buckets;
    """
    spark.sql(medals_matches_players_bucketed_ddl)

    # Save match_details_bucketed table
    medals_matches_players.select("match_id", "player_gamertag", "medal_id", "count") \
        .write.mode("append") \
        .bucketBy(16, "match_id") \
        .saveAsTable("homework.medals_matches_players_bucketed")

def create_medals_table(spark, medals):
    """Create a table medals"""

    # DDL - medals table
    spark.sql("""DROP TABLE IF EXISTS homework.medals""")
    medals_ddl = """
    CREATE TABLE IF NOT EXISTS homework.medals (
        medal_id STRING,
        classification STRING,
        description STRING,
        name STRING
    )
    USING iceberg;
    """
    spark.sql(medals_ddl)

    # Save medals table
    medals.select("medal_id", "classification", "description", "name") \
        .write.mode("append") \
        .saveAsTable("homework.medals")

def create_join_dataframe(spark):
    """Create a joined data frame for aggrgation"""
    join_sql = """
    SELECT
        m.match_id,
        m.mapid as map_id,
        m.playlist_id,
        m_details.player_gamertag,
        m_details.player_total_kills
    FROM homework.matches_bucketed m
    LEFT JOIN homework.match_details_bucketed m_details ON m.match_id = m_details.match_id
    """
    return spark.sql(join_sql)

def create_player_performance_dataframe(spark):
    """
    Create a dataframe that contains players performance
    """
    join_sql = """
    SELECT
        m.match_id,
        m.mapid as map_id,
        m.playlist_id,
        m_details.player_gamertag,
        m_details.player_total_kills,
        medals.classification as medal_classification,
        medal_players.count AS medal_count
    FROM homework.matches_bucketed m
    JOIN homework.match_details_bucketed m_details ON m.match_id = m_details.match_id
    LEFT JOIN homework.medals_matches_players_bucketed medal_players ON medal_players.match_id = m.match_id AND medal_players.player_gamertag = m_details.player_gamertag
    LEFT JOIN homework.medals ON medals.medal_id = medal_players.medal_id
    """
    return spark.sql(join_sql)

def aggregate_most_kills_player_per_game(joined_df):
    """Aggregate - Which player averages the most kills per game?"""
    match_player_kills_df = joined_df \
        .groupBy("player_gamertag") \
        .agg((sum("player_total_kills")/count("match_id")).alias("player_total_kills")) \
        .sort(desc("player_total_kills"))
    match_player_kills_df.head(1)

def aggregate_playlist_played_the_most(joined_df):
    """Aggregate playlist gets played the most"""
    most_playlist_df = joined_df \
        .groupBy("playlist_id") \
        .agg(countDistinct("match_id").alias("count_playlist")) \
        .sort(desc("count_playlist"))
    most_playlist_df.head(1)

def aggregate_map_played_the_most(joined_df):
    """Aggregate which maps gets played the most"""
    most_played_map_df = joined_df \
        .groupBy("map_id").agg(countDistinct("match_id").alias("count_played")) \
        .sort(desc("count_played"))
    most_played_map_df.head(1)

def aggregate_maps_player_get_the_most_killing_spree_on(player_performance_df):
    """Aggregate which maps player get the most Killing Spree on"""
    most_map_has_killing_spree_df = player_performance_df \
        .where(player_performance_df.medal_classification == "KillingSpree") \
        .groupBy("map_id") \
        .agg(sum("medal_count").alias("total_killingspree_medals")) \
        .sort(desc("total_killingspree_medals"))
    most_map_has_killing_spree_df.head(1)

def try_out_sortWithinPartition(spark, player_performance_df):
    """Try out sortWithinPartition"""
    start_df = player_performance_df.repartition(9, col("playlist_id"))
    first_sort_df = start_df.sortWithinPartitions(col("map_id"), col("medal_classification"))

    start_df.write.mode("overwrite").saveAsTable("homework.player_performance_unsorted")
    first_sort_df.write.mode("overwrite").saveAsTable("homework.player_performance_sorted")

    sql = """
        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'sorted' 
        FROM homework.player_performance_sorted.files

        UNION ALL
        SELECT SUM(file_size_in_bytes) as size, COUNT(1) as num_files, 'unsorted' 
        FROM homework.player_performance_unsorted.files
        """

    spark.sql(sql).show()

def main():
    spark = SparkSession.builder.appName("spark-fundamentals-homework").config("spark.executor.memory", "8g").getOrCreate()
    
    # Read the medals data
    medals = spark.read.option("header", "true").csv("/home/iceberg/data/medals.csv")

    # Read the matches data
    matches = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/home/iceberg/data/matches.csv")

    # Read the match details data
    match_details = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/home/iceberg/data/match_details.csv")

    # Read the medals_matches_players data
    medals_matches_players = spark.read.option("header", "true").csv("/home/iceberg/data/medals_matches_players.csv")

    # Read the maps data
    maps = spark.read.option("header", "true").csv("/home/iceberg/data/maps.csv")

    # Disabled automatic broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    matches_and_map = explicitly_broadcast_join_on_maps(matches, maps)
    players_medals = explicitly_broadcast_join_on_medals(medals_matches_players, medals)

    create_matches_bucketed(spark, matches)
    create_match_details_bucketed(spark, match_details)
    create_medals_matches_players_bucketed(spark, medals_matches_players)
    create_medals_table(spark, medals)

    # Create joined dataframe for aggregation
    joined_df = create_join_dataframe()
    player_performance_df = create_player_performance_dataframe()

    # Execute Aggregation
    aggregate_most_kills_player_per_game(joined_df)
    aggregate_playlist_played_the_most(joined_df)
    aggregate_map_played_the_most(joined_df)
    aggregate_maps_player_get_the_most_killing_spree_on(player_performance_df)

    try_out_sortWithinPartition(spark, player_performance_df)
