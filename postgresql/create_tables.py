import psycopg2
from psycopg2 import sql, OperationalError
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    Drops (if exists) and creates the sparkifydb database.
    Returns the connection and cursor to sparkifydb.
    """
    try:
        # Connect to default database
        conn = psycopg2.connect(
            host="127.0.0.1",
            dbname="studentdb",
            user="student",
            password="student"
        )
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # Create sparkify database with UTF8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb;")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0;")

        # Close connection to default database
        conn.close()

        # Connect to sparkify database
        conn = psycopg2.connect(
            host="127.0.0.1",
            dbname="sparkifydb",
            user="student",
            password="student"
        )
        cur = conn.cursor()

        return cur, conn

    except OperationalError as e:
        print(f"Error creating database: {e}")
        raise


def drop_tables(cur, conn):
    """
    Drops all tables using queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables using queries in `create_table_queries` list.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Creates the sparkifydb database.
    - Drops all existing tables.
    - Creates new tables.
    - Closes the database connection.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
