CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    gender VARCHAR(10),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    title VARCHAR(50),
    email VARCHAR(255),
    username VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    street_name VARCHAR(255),
    street_number VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postcode VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS logins (
    id SERIAL PRIMARY KEY,
    username VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    phone VARCHAR(50),
    cell VARCHAR(50)
);
