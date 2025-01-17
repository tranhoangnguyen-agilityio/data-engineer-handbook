-- An incremental query that load table host_activity_reduced
WITH
	daily_aggregate AS (
		SELECT
		 	host,
			DATE(event_time) as date,
			COUNT(1) AS num_host_hits,
			COUNT(DISTINCT user_id) AS num_unique_visitors
		FROM events
		WHERE DATE(event_time) = DATE('2023-01-03')
		GROUP BY host, DATE(event_time)
	),
	yesterday_array AS (
		SELECT
			*
		FROM host_activity_reduced
		WHERE month_start = DATE('2023-01-01')
	)


-- Insert aggregated metrics into the host_activity_reduced table
INSERT INTO host_activity_reduced
SELECT
	-- Determine month_start date
	COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
	-- Select host from either daily_aggregate or yesterday_array
    COALESCE( da.host, ya.host) AS host,
	CASE
        WHEN ya.hit_array IS NOT NULL THEN 
            ya.hit_array || ARRAY[COALESCE(da.num_host_hits,0)] 
        WHEN ya.hit_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) 
                || ARRAY[COALESCE(da.num_host_hits,0)]
	END as hit_array,
	CASE
        WHEN ya.unique_visitors_array IS NOT NULL THEN 
            ya.unique_visitors_array || ARRAY[COALESCE(da.num_unique_visitors,0)] 
        WHEN ya.unique_visitors_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) 
                || ARRAY[COALESCE(da.num_unique_visitors,0)]
	END as unique_visitors_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya ON da.host = ya.host
ON CONFLICT (month_start, host)
DO UPDATE
SET
	hit_array = EXCLUDED.hit_array,
	unique_visitors_array = EXCLUDED.unique_visitors_array;
