-- Query that populates the actors table one year at a time
WITH
	last_year AS (
		SELECT * FROM actors WHERE current_year = 1988
	),
	this_year AS (
		SELECT 
			actor,
			actorid,
			array_agg(ROW(film, votes, rating, filmid)::film_stats) as films,
			CASE
				WHEN AVG(rating) > 8 THEN 'star'
				WHEN AVG(rating) > 7 THEN 'good'
				WHEN AVG(rating) > 6 THEN 'average'
				ELSE 'bad'
			END::quality_class as quality_class,
			year
		FROM actor_films
		WHERE year = 1989
		-- Grouping the results by actor, actor_id, and year for aggregation
		GROUP BY actor, actorid, year
	)

INSERT INTO actors
SELECT
	COALESCE(ly.actor, ty.actor) AS actor,
	COALESCE(ly.actorid, ty.actorid) AS actorid,
	COALESCE(ly.films,
		ARRAY[]::film_stats[]
	) || COALESCE(ty.films,
		ARRAY[]::film_stats[]
	) as films,
	COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
	ty.actorid IS NOT NULL as is_active,
	COALESCE(ty.year, ly.current_year + 1) as current_year
FROM last_year ly
FULL OUTER JOIN this_year ty ON ly.actor = ty.actor

