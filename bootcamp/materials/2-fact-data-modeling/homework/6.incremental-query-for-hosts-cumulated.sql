WITH
	yesterday AS (
		SELECT 
			*
		FROM hosts_cumulated
		WHERE date = DATE('2023-01-01')
	),
	today AS (
		SELECT
			e.host,
			DATE(MIN(event_time)) as date_access,
			count(1) as host_access_count
		FROM events e
		WHERE DATE(event_time) = DATE('2023-01-02')
		GROUP BY e.host, DATE(event_time)
	)

INSERT INTO hosts_cumulated
SELECT 
	COALESCE (t.host, y.host) as host,
	CASE
		WHEN y.host_activity_datelist is NULL THEN ARRAY[t.date_access]
		WHEN t.date_access is NULL THEN y.host_activity_datelist
		ELSE ARRAY[t.date_access] || y.host_activity_datelist
	END as host_activity_datelist,
	COALESCE (t.date_access, y.date + INTERVAL '1 day') as date
FROM yesterday y
FULL OUTER JOIN today t on y.host = t.host
