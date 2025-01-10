
-- CREATE TYPE actor_scd_type AS (
-- 	quality_class quality_class,
-- 	is_active boolean,
-- 	start_date INTEGER,
-- 	end_date INTEGER
-- )
						
WITH
	last_year_scd AS (
		SELECT * FROM actors_history_scd WHERE current_year = 1988 AND end_date = 1988
	),
	historical_scd AS (
		SELECT * FROM actors_history_scd WHERE current_year = 1988 AND end_date < 1988
	),
	current_year_data AS (
		SELECT * FROM actors WHERE current_year = 1989
	),
	unchanged_records AS (
		SELECT
			cy.actor,
			cy.quality_class,
			cy.is_active,
			ly.start_date,
			cy.current_year as end_year
		FROM current_year_data cy
		INNER JOIN last_year_scd ly ON cy.actor = ly.actor
		WHERE ly.is_active = cy.is_active
		AND ly.quality_class = cy.quality_class
	),
	changed_records AS (
		SELECT
			cy.actor,
			UNNEST(ARRAY[
				ROW(
					ly.quality_class,
					ly.is_active,
					ly.start_date,
					ly.end_date
				)::actor_scd_type,
				ROW(
					cy.quality_class,
					cy.is_active,
					cy.current_year,
					cy.current_year
				)::actor_scd_type
			]) as records
		FROM current_year_data cy
		LEFT JOIN last_year_scd ly ON cy.actor = ly.actor
		WHERE (
			ly.is_active <> cy.is_active
			OR ly.quality_class <> cy.quality_class
		)
	),
	unnested_changed_records AS (
		SELECT
			actor,
			(records::actor_scd_type).quality_class,
			(records::actor_scd_type).is_active,
			(records::actor_scd_type).start_date,
			(records::actor_scd_type).end_date
		FROM changed_records
	),
	new_records AS (
		SELECT
			cy.actor,
			cy.quality_class,
			cy.is_active,
			cy.current_year as start_date,
			cy.current_year as end_date
		FROM current_year_data cy
		LEFT JOIN last_year_scd ly ON cy.actor = ly.actor
		WHERE ly.actor is NULL
	)

INSERT INTO actors_history_scd
SELECT *, 1989 as current_year FROM unchanged_records
UNION ALL
SELECT *, 1989 as current_year FROM unnested_changed_records
UNION ALL
SELECT *, 1989 as current_year FROM new_records
