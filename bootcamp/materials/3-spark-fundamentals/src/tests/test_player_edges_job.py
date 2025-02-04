from pyspark.sql import Row
from chispa import assert_df_equality

from ..jobs.player_player_edges_job import do_aggregate_game_details 

def test_do_aggregate_game_details(spark):
    # Input DataFrame (mock game details)
    input_data = [
        Row(player_id="p1", player_name="Alice", game_id="g1", team_abbreviation="A", pts=20),
        Row(player_id="p2", player_name="Bob", game_id="g1", team_abbreviation="B", pts=15),
        Row(player_id="p3", player_name="Charlie", game_id="g1", team_abbreviation="A", pts=10),
        Row(player_id="p1", player_name="Alice", game_id="g2", team_abbreviation="A", pts=25),
        Row(player_id="p2", player_name="Bob", game_id="g2", team_abbreviation="B", pts=30),
    ]
    input_df = spark.createDataFrame(input_data)

    # Expected DataFrame after aggregation
    expected_data = [
        Row(
            player_id="p2",
            player_name="Bob",
            player_id_1="p1",
            player_name_1="Alice",
            edge_type="plays_against",
            num_games=2,
            left_points=45,
            right_points=45 
        ),
        Row(
            player_id="p3",
            player_name="Charlie",
            player_id_1="p1",
            player_name_1="Alice",
            edge_type="shares_team",
            num_games=1,
            left_points=10,
            right_points=20
        ),
        Row(
            player_id="p3",
            player_name="Charlie",
            player_id_1="p2",
            player_name_1="Bob",
            edge_type="plays_against",
            num_games=1,
            left_points=10,
            right_points=15
        )
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Run the transformation
    result_df = do_aggregate_game_details(spark, input_df)

    # Assert the result matches the expected DataFrame
    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)
