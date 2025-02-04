from pyspark.sql import Row
from chispa import assert_df_equality

from ..jobs.player_game_edges_job import do_game_details_transformation

def test_do_game_details_transformation(spark):
    # Input DataFrame (mock data)
    input_data = [
        Row(player_id="p1", game_id="g1", start_position="forward", pts=20, team_id="t1", team_abbreviation="A"),
        Row(player_id="p1", game_id="g1", start_position="guard", pts=15, team_id="t1", team_abbreviation="A"),  # Duplicate to be deduped
        Row(player_id="p2", game_id="g2", start_position="center", pts=30, team_id="t2", team_abbreviation="B")
    ]
    input_df = spark.createDataFrame(input_data)

    # Expected DataFrame after transformation
    expected_data = [
        Row(
            subject_identifier="p1",
            subject_type="player",
            object_identifier="g1",
            object_type="game",
            edge_type="plays_in",
            # properties='{"start_position":"forward","pts":20,"team_id":"t1","team_abbreviation":"A"}'
        ),
        Row(
            subject_identifier="p2",
            subject_type="player",
            object_identifier="g2",
            object_type="game",
            edge_type="plays_in",
            # properties='{"start_position":"center","pts":30,"team_id":"t2","team_abbreviation":"B"}'
        )
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Run the transformation
    result_df = do_game_details_transformation(spark, input_df)

    # Assert the result matches the expected DataFrame
    assert_df_equality(result_df, expected_df, ignore_row_order=True, ignore_nullable=True)
