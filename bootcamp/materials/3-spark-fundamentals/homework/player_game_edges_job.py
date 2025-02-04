from pyspark.sql import SparkSession

def do_game_details_transformation(spark, dataframe):
    query = f"""
    WITH deduped AS (
        SELECT *, row_number() over (PARTITION BY player_id, game_id ORDER BY player_id) AS row_num
        FROM game_details
    )
    SELECT
        player_id AS subject_identifier,
        'player' as subject_type,
        game_id AS object_identifier,
        'game' AS object_type,
        'plays_in' AS edge_type
    FROM deduped
    WHERE row_num = 1
    """
    dataframe.createOrReplaceTempView('game_details')
    return spark.sql(query)

def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("homework_spark_fundamentals") \
      .getOrCreate()
    _ = do_game_details_transformation(spark, spark.table("game_details"))
