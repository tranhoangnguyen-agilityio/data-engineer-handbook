-- Common Table Expressions (CTEs) for breaking the query into logical steps
WITH
	-- CTE `yesterday`: Retrieves all records from `user_devices_cumulated` for the specific date '2023-01-30'.
	yesterday AS (
		SELECT 
			*
		FROM user_devices_cumulated
		WHERE date = DATE('2023-01-30')
	),
	-- CTE `today`: Aggregates event data for January 31, 2023.
	today AS (
		SELECT
			e.user_id,
			d.browser_type,
			DATE(MIN(event_time)) as date_active,
			count(1) as event_count
		FROM events e
		INNER JOIN devices d ON e.device_id = d.device_id
		WHERE user_id IS NOT NULL 
		AND DATE(event_time) = DATE('2023-01-31')
		GROUP BY user_id, d.browser_type, DATE(event_time)
	)

-- Insert the combined results into the `user_devices_cumulated` table
INSERT INTO user_devices_cumulated
SELECT 
	COALESCE (t.user_id, y.user_id) as user_id,
	COALESCE (t.browser_type, y.browser_type) as browser_type,
	CASE
		-- If there is no activity in `yesterday`, initialize with today's active date
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
		-- If there is no activity in `today`, keep the previous activity list
		WHEN y.device_activity_datelist is NULL THEN ARRAY[t.date_active]
		-- Otherwise, concatenate today's active date with the previous activity list
		WHEN t.date_active is NULL THEN y.device_activity_datelist
		ELSE ARRAY[t.date_active] || y.device_activity_datelist
	END as device_activity_datelist,
	COALESCE (t.date_active, y.date + INTERVAL '1 day') as date
FROM yesterday y
FULL OUTER JOIN today t on y.user_id = t.user_id AND y.browser_type = t.browser_type
-- The FULL OUTER JOIN ensures that:
-- - All rows from `yesterday` are included, even if there's no match in `today`.
-- - All rows from `today` are included, even if there's no match in `yesterday`.
