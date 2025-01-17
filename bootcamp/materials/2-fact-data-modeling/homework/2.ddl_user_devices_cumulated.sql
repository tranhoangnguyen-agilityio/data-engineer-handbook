CREATE TABLE user_devices_cumulated (
	user_id numeric,
	-- The browser type that users use to access.
	browser_type TEXT,
	-- The list of dates in the past where the user device had accessed.
	device_activity_datelist DATE[],
	-- The current date for the user.
	date DATE,
	PRIMARY KEY (user_id, browser_type, date)
)