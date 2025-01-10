CREATE TYPE film_stats AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

CREATE TYPE quality_class AS
	ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	-- Array for multiple films of each actor. Each row contains film details
	films film_stats[],
	-- Rating based on average rating in the most recent year
	quality_class quality_class,
	-- Indicates if the actor is currently active if this year
	is_active boolean,
	-- Represents the year this row is relevant for the actor
	current_year INTEGER,
	PRIMARY KEY (actor, current_year)
);
