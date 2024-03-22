CREATE TABLE user_connection_table (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id VARCHAR(50),
  token_info TEXT
);

CREATE TABLE importing_job_table (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id VARCHAR(50),
  object_name VARCHAR(30),
  start_date VARCHAR(10),
  last_date VARCHAR(10),
  active BOOLEAN DEFAULT TRUE
);
