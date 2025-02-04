-- Survival analysis

-- Survival analysis - The daily user active of the cohort start in 2023-01-01
SELECT
	date - first_active_date AS days_since_first_active,
	COUNT(CASE
		WHEN daily_active_state IN ('Retained', 'Resurrected', 'New') THEN 1 
	END) as number_active,
	COUNT(1) as total_users,
	CAST(
		COUNT(CASE
			WHEN daily_active_state
				IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL) / COUNT(1) AS active_user_ratio
FROM users_growth_accounting
WHERE first_active_date = DATE('2023-01-01')
GROUP BY date - first_active_date
ORDER BY date - first_active_date

-- By removing the condition, we can have the Survival analysis across cohorts
SELECT
	date - first_active_date AS days_since_first_active,
	COUNT(CASE
		WHEN daily_active_state IN ('Retained', 'Resurrected', 'New') THEN 1 
	END) as number_active,
	COUNT(1) as total_users,
	CAST(
		COUNT(CASE
			WHEN daily_active_state
				IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL) / COUNT(1) AS active_user_ratio
FROM users_growth_accounting
GROUP BY date - first_active_date
ORDER BY date - first_active_date

-- Group by the DOW (Day of Week) to see the which days users active the most
SELECT
	extract(dow from first_active_date) as dow,
	date - first_active_date AS days_since_first_active,
	COUNT(CASE
		WHEN daily_active_state IN ('Retained', 'Resurrected', 'New') THEN 1 
	END) as number_active,
	COUNT(1) as total_users,
	CAST(
		COUNT(CASE
			WHEN daily_active_state
				IN ('Retained', 'Resurrected', 'New') THEN 1 END) AS REAL) / COUNT(1) AS active_user_ratio
FROM users_growth_accounting
GROUP BY extract(dow from first_active_date), date - first_active_date
ORDER BY date - first_active_date