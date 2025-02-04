from pyspark.sql import SparkSession

def do_aggregate_game_details(spark, dataframe):
    query = f"""
    WITH deduped AS (
        SELECT *, row_number() over (PARTITION BY player_id, game_id ORDER BY player_id) AS row_num
        FROM game_details
    ),
        filtered AS (
            SELECT * FROM deduped
            WHERE row_num = 1
        ),
        aggregated AS (
            SELECT
            f1.player_id AS player_id,
            f1.player_name AS player_name,
            f2.player_id AS player_id_1,
            f2.player_name AS player_name_1, 
            CASE WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN 'shares_team'
            ELSE 'plays_against'
            END AS edge_type,
            COUNT(1) AS num_games,
            SUM(f1.pts) AS left_points,
            SUM(f2.pts) as right_points
        FROM filtered f1
        JOIN filtered f2
        ON f1.game_id = f2.game_id
        AND f1.player_name <> f2.player_name
        WHERE f1.player_id > f2.player_id
        GROUP BY
            f1.player_id,
            f1.player_name,
            f2.player_id,
            f2.player_name,
            CASE WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN  'shares_team'
            ELSE 'plays_against'
            END
        )
    SELECT * FROM aggregated
    """
    dataframe.createOrReplaceTempView('game_details')
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("homework_spark_fundamentals") \
      .getOrCreate()
    _ = do_aggregate_game_details(spark, spark.table("game_details"))

