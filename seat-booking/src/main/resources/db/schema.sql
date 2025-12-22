-- Create users table
CREATE TABLE IF NOT EXISTS users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

-- Create seats table
CREATE TABLE IF NOT EXISTS seats (
    seat_number VARCHAR(10) PRIMARY KEY,
    user_id INTEGER NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
