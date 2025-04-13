"""
Script to initialize Cassandra keyspace and tables for the Messenger application.
"""
import os
import time
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

def wait_for_cassandra():
    """Wait for Cassandra to be ready before proceeding."""
    logger.info("Waiting for Cassandra to be ready...")
    cluster = None

    for _ in range(10):  # Try 10 times
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            logger.info("Cassandra is ready!")
            return cluster
        except Exception as e:
            logger.warning(f"Cassandra not ready yet: {str(e)}")
            time.sleep(5)  # Wait 5 seconds before trying again

    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise Exception("Could not connect to Cassandra")

def create_keyspace(session):
    """
    Create the keyspace if it doesn't exist.

    This is where students will define the keyspace configuration.
    """
    logger.info(f"Creating keyspace {CASSANDRA_KEYSPACE} if it doesn't exist...")
    query = """
    CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{
        'class': 'SimpleStrategy',
        'replication_factor': 1
    }}""".format(CASSANDRA_KEYSPACE)
    session.execute(query)

    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} is ready.")

def create_tables(session):
    """
    Create the tables for the application.

    This is where students will define the table schemas based on the requirements.
    """
    logger.info("Creating tables...")

    session.set_keyspace('messenger')

    # Create messages_by_conversation table
    session.execute("""
        CREATE TABLE IF NOT EXISTS messages_by_conversation (
            conversation_id int,
            created_at timestamp,
            id uuid,
            sender_id int,
            receiver_id int,
            content text,
            PRIMARY KEY ((conversation_id), created_at, id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, id DESC)
    """)

    # Create conversations table
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            id int PRIMARY KEY,
            user1_id int,
            user2_id int,
            last_message_at timestamp,
            last_message_content text
        )
    """)

    # Create conversations_by_user table
    session.execute("""
        CREATE TABLE IF NOT EXISTS conversations_by_user (
            user_id int,
            conversation_id int,
            last_message_at timestamp,
            last_message_content text,
            PRIMARY KEY ((user_id), last_message_at, conversation_id)
        ) WITH CLUSTERING ORDER BY (last_message_at DESC)
    """)

    logger.info("✅ All tables created successfully.")


    # TODO: Students should implement table creation
    # Hint: Consider:
    # - What tables are needed to implement the required APIs?
    # - What should be the primary keys and clustering columns?
    # - How will you handle pagination and time-based queries?

    logger.info("Tables created successfully.")

def main():
    """Initialize the database."""
    logger.info("Starting Cassandra initialization...")

    # Wait for Cassandra to be ready
    cluster = wait_for_cassandra()

    try:
        # Connect to the server
        session = cluster.connect()

        # Create keyspace and tables
        create_keyspace(session)
        session.set_keyspace(CASSANDRA_KEYSPACE)
        create_tables(session)

        logger.info("Cassandra initialization completed successfully.")
    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise
    finally:
        if cluster:
            cluster.shutdown()

if __name__ == "__main__":
    main()