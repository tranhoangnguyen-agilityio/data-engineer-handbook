-- Find users that visit the signup page and really sign up.
WITH
	deduped_events AS (
		SELECT
			user_id, url, event_time, DATE(event_time) as event_date
		FROM events
		WHERE user_id IS NOT NULL
		AND url IN ('/signup', '/api/v1/login')
		GROUP BY user_id, url, event_time, DATE(event_time)
),
	selfjoined AS (
		SELECT
			d1.user_id, d1.url, d2.url as destination_url, d1.event_time, d2.event_time
		FROM deduped_events d1
		JOIN deduped_events d2 ON d1.user_id = d2.user_id 
		AND d1.event_date = d2.event_date
		AND d2.event_time > d1.event_time
		WHERE d1.url = '/signup'
),
	userlevel AS (
		SELECT
			user_id,
			MAX(CASE WHEN destination_url = '/api/v1/login' THEN 1 ELSE 0 END) AS converted	
		FROM selfjoined
		GROUP BY user_id
)

-- Overral converted rate is the total user really sign up after visiting the signup page
SELECT CAST(SUM(converted) as REAL) / COUNT(1) as pct_converted
FROM userlevel