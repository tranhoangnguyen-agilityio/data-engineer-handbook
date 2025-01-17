INSERT INTO users_cumulated
WITH yesterday AS (
	SELECT
		*
	FROM users_cumulated
	WHERE date = DATE('2023-01-30')
),
	today AS (
		SELECT
			CAST(user_id AS TEXT) as user_id,
			DATE(CAST(event_time as TIMESTAMP)) AS date_active
		FROM events
		WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-31')
		AND user_id IS NOT NULL
		GROUP BY user_id, date_active
	)

SELECT
	COALESCE (t.user_id, y.user_id) as user_id,
	CASE
		WHEN y.dates_active is NULL THEN ARRAY[t.date_active]
		WHEN t.date_active is NULL THEN y.dates_active
		ELSE ARRAY[t.date_active] || y.dates_active
		END
	AS dates_active,
	COALESCE (t.date_active, y.date + INTERVAL '1 day') as date
FROM yesterday y
FULL OUTER JOIN today t ON y.user_id = t.user_id 

SELECT * FROM users_cumulated
WHERE date = DATE('2023-01-31')

-- CREATE TABLE users_cumulated (
-- 	user_id TEXT,
-- 	-- The list of dates in the past where the user was active
-- 	dates_active DATE[],
-- 	-- The current date for the user
-- 	date DATE,
-- 	PRIMARY KEY (user_id, date)
-- )


-- SELECT min(event_time) FROM events
