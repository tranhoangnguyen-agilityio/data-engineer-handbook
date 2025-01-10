WITH
	with_previous AS (
		SELECT
			actor,
			current_year,
			quality_class,
			LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) as previous_quality_class,
			CASE
				WHEN is_active THEN 1
				ELSE 0
			END AS is_active,
			CASE
				WHEN LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) THEN 1
				ELSE 0
			END as previous_is_active
		FROM actors
		WHERE current_year <= 1988
	),
	with_indicators AS (
		SELECT
			*,
			CASE
				WHEN quality_class <> previous_quality_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END AS change_indicator
		FROM with_previous
	),
	with_streaks AS (
		SELECT *,
			SUM(change_indicator)
				OVER (PARTITION BY actor ORDER BY current_year) as streak_identifier
		FROM with_indicators
	)

INSERT INTO actors_history_scd
SELECT
	actor,
	MAX(quality_class)::quality_class as quality_class,
	MAX(is_active) = 1 as is_active,
	MIN(current_year) as start_date,
	MAX(current_year) as end_date,
	-- Setting the 'current_year' for all records to 1988
	1988 as current_year
FROM with_streaks
GROUP BY actor, streak_identifier
ORDER BY actor, streak_identifier
	
	
