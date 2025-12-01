CREATE TABLE user_session_analysis (
	session_id VARCHAR(50) NOT NULL,
	user_id VARCHAR(50) DISTKEY,
	user_country VARCHAR(50),
	user_device VARCHAR(50),
	session_start_time TIMESTAMP SORTKEY,
	total_events INT DEFAULT 0,
	event_type VARCHAR(100),
	transaction_id VARCHAR(50),
	amount DECIMAL(10,2),
	currency VARCHAR(3),
	is_conversion BOOLEAN DEFAULT FALSE
);