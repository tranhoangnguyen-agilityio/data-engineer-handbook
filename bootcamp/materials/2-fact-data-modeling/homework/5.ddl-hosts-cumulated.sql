 CREATE TABLE hosts_cumulated (
	host VARCHAR,
	-- The list of dates in the past where the host had accessed.
	host_activity_datelist DATE[],
	--  The current date for the user
	date DATE,
	PRIMARY KEY (host, date)
)