import psycopg2
from utils.constants import (
    DATABASE_HOST,
    DATABASE_NAME,
    DATABASE_USER,
    DATABASE_PASSWORD,
    DATABASE_PORT,
)

def test_connection():
    print("Connecting to Redshift...")
    conn = psycopg2.connect(
        host=DATABASE_HOST,
        dbname=DATABASE_NAME,
        user=DATABASE_USER,
        password=DATABASE_PASSWORD,
        port=DATABASE_PORT,
        connect_timeout=10
    )
    cur = conn.cursor()
    cur.execute("SELECT current_user, current_database();")
    print(cur.fetchall())
    cur.close()
    conn.close()
    print("✅ Redshift connection successful")

if __name__ == "__main__":
    test_connection()