-- CTE to remove duplicated data
WITH deduped AS (
	SELECT
		g.game_date_est,
		g.season,
		g.home_team_id,
		gd.*,
		-- Determine the WHEN in Fact data
		ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) as row_num
		
	FROM game_details gd
		JOIN games g on gd.game_id = g.game_id
	-- Filter just for speed up in the lab.
	-- WHERE g.game_date_est = '2016-10-04'
)

INSERT INTO fct_game_details
SELECT 
	-- Game table
	game_date_est as dim_game_date,
	-- Most of games table columns are aggregate columns that can be aggregate from game_details table.
	season as dim_season,
	team_id as dim_team_id,
	-- Game detail table
	player_id as dim_player_id,
	player_name as dim_player_name,
	start_position as dim_start_position,
	team_id = home_team_id AS dim_is_playing_at_home,
	-- Comment meaning
	-- DNP: Did Not Played
	-- DND: Did Not Dressed
	-- NWT: Not With Team
	COALESCE(POSITION('DNP' in comment), 0 ) > 0
		AS dim_did_not_play,
	COALESCE(POSITION('DND' in comment), 0 ) > 0
		AS dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment), 0 ) > 0
		AS dim_not_with_team,
	CAST(SPLIT_PART(min, ':', 1) AS REAL)
		+ CAST(SPLIT_PART(min, ':', 2) AS REAL)/60
		AS minutes,
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
	-- We dont need other columns such as team_abbreviation as we can join this data from team table. And this join is cheap as the number of teams is small.
FROM deduped
WHERE row_num = 1

CREATE TABLE fct_game_details (
	-- Sometimes you want to label the columns either  as measures or as dimensions. For example:
	-- game_date → dim_game_date
	-- If you put dim_ that means these are columns that you should filter on and group by on
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_start_position TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_did_not_with_team BOOLEAN,
	-- If you put m_ that means these are columns that you should aggregate and do all sort of maths
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
)

