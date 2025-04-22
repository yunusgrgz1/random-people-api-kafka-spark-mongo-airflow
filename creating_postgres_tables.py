import psycopg2

conn = psycopg2.connect(
    dbname="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432"
)

cursor = conn.cursor()

# Create users table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        gender VARCHAR(10),
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        title VARCHAR(50),
        email VARCHAR(255),
        username VARCHAR(100)
    );
""")

# Create addresses table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS addresses (
        id SERIAL PRIMARY KEY,
        street_name VARCHAR(255),
        street_number VARCHAR(50),
        city VARCHAR(100),
        state VARCHAR(100),
        country VARCHAR(100),
        postcode VARCHAR(20)
    );
""")

# Create logins table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS logins (
        id SERIAL PRIMARY KEY,
        username VARCHAR(100)
    );
""")

# Create contacts table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS contacts (
        id SERIAL PRIMARY KEY,
        phone VARCHAR(50),
        cell VARCHAR(50)
    );
""")

conn.commit()
cursor.close()
conn.close()
