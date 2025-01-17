 -- Convert the device_activity_datelist column into a datelist_int column
 WITH 
 	users AS (
		 SELECT
		 	*
		 FROM user_devices_cumulated
		 WHERE date = DATE('2023-01-31')
	),
	series AS (
		SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') as series_date
	),
	place_holder_ints AS (
		SELECT
			31 - (date - DATE(series_date)) as date_diff_from_31,
			-- The @> operator checks if an array contains another array (i.e., all elements of the second array are in the first array).
			CASE
				WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
					THEN POW(2, 30 - (date - DATE(series_date)))
				ELSE 0
			END as placeholder_int_value,
			*
		FROM users CROSS JOIN series
	),
	place_holder_ints_by_user_browsers AS (
		SELECT
			user_id,
			SUM(placeholder_int_value) AS sum_placeholder_int_value,
			CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(31)) as placeholder_int_value_aggregated
		FROM place_holder_ints
		GROUP BY user_id, browser_type
	)

SELECT
	user_id,
	placeholder_int_value_aggregated,
	BIT_COUNT(placeholder_int_value_aggregated) > 0 AS dim_is_monthly_active,
	BIT_COUNT(CAST('1111111000000000000000000000000' AS BIT(31)) & placeholder_int_value_aggregated) > 0 as dim_is_weekly_active
FROM place_holder_ints_by_user_browsers