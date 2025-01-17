-- CREATE TABLE array_metrics (
-- 	user_id NUMERIC,
-- 	month_start DATE,
-- 	metric_name TEXT,
-- 	metric_array REAL[],
-- 	PRIMARY KEY (user_id, month_start, metric_name)
-- )

WITH
	daily_aggregate AS (
		SELECT
			user_id,
			DATE(event_time) as date,
			COUNT(1) as num_site_hits
		FROM events
		WHERE DATE(event_time) = DATE('2023-01-03')
		AND user_id IS NOT NULL
		GROUP BY user_id, DATE(event_time)
	),
	yesterday_array AS (
		SELECT *
		FROM array_metrics
		WHERE month_start = DATE('2023-01-01')
	)

-- Insert aggregated metrics into the array_metrics table
INSERT INTO array_metrics
SELECT
    -- Select user_id from either daily_aggregate or yesterday_array
    COALESCE( da.user_id, ya.user_id) AS user_id,
	-- Determine month_start date
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    -- Set metric name to 'site_hits'
    'site_hits' AS metric_name,
    -- Update metric_array based on existing data and new daily aggregates
    CASE 
        WHEN ya.metric_array IS NOT NULL THEN 
            ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)] 
        WHEN ya.metric_array IS NULL THEN
            ARRAY_FILL(0, ARRAY[COALESCE (date - DATE(DATE_TRUNC('month', date)), 0)]) 
                || ARRAY[COALESCE(da.num_site_hits,0)]
    END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO 
	UPDATE SET metric_array = EXCLUDED.metric_array;


-- Turn the monthly aggregate to daily aggregate
-- Aggregate metrics by summing specific elements in the metric_array
WITH agg AS (
    SELECT metric_name, month_start, ARRAY[SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
    FROM array_metrics
    GROUP BY metric_name, month_start
)
-- Select and display the metric_name, date (adjusted by index), and summed value
SELECT 
    metric_name, 
    month_start + CAST(CAST(index - 1 AS TEXT) || ' day' AS INTERVAL) AS adjusted_date,
    elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index);