SELECT
	dim_player_name,
	COUNT(1) AS num_games,
	COUNT(CASE WHEN dim_did_not_with_team THEN 1 END) as bailed_count,
	CAST(COUNT(CASE WHEN dim_did_not_with_team THEN 1 END) AS REAL)/ COUNT(1) AS bail_pc
FROM fct_game_details
GROUP BY dim_player_name
ORDER BY bail_pc DESC