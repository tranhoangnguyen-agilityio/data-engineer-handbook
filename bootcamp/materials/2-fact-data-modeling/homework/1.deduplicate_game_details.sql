-- CTE to remove duplicated data
WITH deduped AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
	FROM game_details
)

SELECT
	game_id,
	team_id,
	player_id,
	player_name,
	start_position,
	fgm AS m_fgm,
	fga AS m_fga,
	fg3m AS m_fg3m,
	fg3a AS m_fg3a,
	ftm AS m_ftm,
	fta AS m_fta,
	oreb AS m_oreb,
	dreb AS m_dreb,
	reb AS m_reb,
	ast AS m_ast,
	stl AS m_stl,
	blk AS m_blk,
	"TO" as m_tunerovers,
	pf AS m_pf,
	pts AS m_pts,
	plus_minus AS m_plus_minus
FROM deduped WHERE row_num = 1
