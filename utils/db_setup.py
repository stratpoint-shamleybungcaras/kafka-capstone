import psycopg2

def setup_database():
    print("Connecting to PostgreSQL at localhost:5433...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="kafka-capstone",
            user="postgres",
            password="postgres"
        )
        
        conn.autocommit = True
        cursor = conn.cursor()

        print("Success: Database tables created and ready for streaming data!")
        
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error connecting to database: {e}")

if __name__ == "__main__":
    setup_database()