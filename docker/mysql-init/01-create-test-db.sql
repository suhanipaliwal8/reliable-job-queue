-- Runs once, the first time the MySQL container creates its data directory.
CREATE DATABASE IF NOT EXISTS jobqueue_test;
GRANT ALL PRIVILEGES ON jobqueue_test.* TO 'jobqueue'@'%';