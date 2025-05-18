import mysql.connector
from mysql.connector import errorcode
import csv
import uuid

DB_NAME = "ALX_prodev"

TABLES = {
    "user_data": (
        "CREATE TABLE IF NOT EXISTS user_data ("
        "  user_id CHAR(36) PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL,"
        "  age DECIMAL NOT NULL,"
        "  INDEX(user_id)"
        ") ENGINE=InnoDB"
    )
}

def connect_db():
    """Connect to MySQL server"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234xxxxx1234"
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def create_database(connection):
    """Create ALX_prodev database"""
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        print(f"Database {DB_NAME} created or exists already")
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Database creation failed: {err}")

def connect_to_prodev():
    """Connect to ALX_prodev database"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234xxxxx1234",
            database=DB_NAME
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to {DB_NAME}: {err}")
        return None

def create_table(connection):
    """Create user_data table"""
    try:
        cursor = connection.cursor()
        cursor.execute(TABLES["user_data"])
        print("Table user_data created successfully")
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Table creation failed: {err}")

def insert_data(connection, csv_file):
    """Insert data from CSV into user_data table"""
    try:
        cursor = connection.cursor()
        with open(csv_file, newline='') as file:
            reader = csv.DictReader(file)
            for row in reader:
                # Avoid duplicates by checking if UUID exists
                user_id = str(uuid.UUID(row['user_id']))
                cursor.execute(
                    "SELECT user_id FROM user_data WHERE user_id = %s", (user_id,)
                )
                if not cursor.fetchone():
                    cursor.execute(
                        "INSERT INTO user_data (user_id, name, email, age) VALUES (%s, %s, %s, %s)",
                        (user_id, row['name'], row['email'], row['age'])
                    )
        connection.commit()
        print(f"Data inserted from {csv_file}")
        cursor.close()
    except Exception as e:
        print(f"Data insertion error: {e}")