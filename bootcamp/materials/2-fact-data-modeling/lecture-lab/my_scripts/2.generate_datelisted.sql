WITH
	users AS (
		SELECT
			*
		FROM users_cumulated
		WHERE date = '2023-01-31'
	),
	series AS (
		SELECT * FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') as series_date
	),
	place_holder_ints AS (
		SELECT
			-- The @> operator checks if an array contains another array (i.e., all elements of the second array are in the first array).
			CASE
				WHEN dates_active @> ARRAY[DATE(series_date)]
					THEN POW(2, 32 - (date - DATE(series_date)))
				ELSE 0
			END as placeholder_int_value,
			*
		FROM users CROSS JOIN series
	),
	place_holder_ints_by_users AS (
		SELECT
			user_id,
			CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) as placeholder_int_value_aggregated
		FROM place_holder_ints
		GROUP BY user_id
	)
	

SELECT
	user_id,
	placeholder_int_value_aggregated,
	BIT_COUNT(placeholder_int_value_aggregated) > 0 AS dim_is_monthly_active,
	BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & placeholder_int_value_aggregated) > 0 as dim_is_weekly_active
FROM place_holder_ints_by_users


