"""
Script to generate test data for the Messenger application.
This script populates Cassandra based on the designed schema.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta, timezone
import time

from cassandra.cluster import Cluster, Session
from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel
from cassandra.util import uuid_from_time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10
NUM_CONVERSATIONS = 15
MAX_MESSAGES_PER_CONVERSATION = 50
MIN_MESSAGES_PER_CONVERSATION = 5

# Consistency Level for Writes
WRITE_CONSISTENCY = ConsistencyLevel.ONE # Use ONE for faster bulk loading, QUORUM for safer writes

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = None
    for i in range(5): # Retry connection
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            logger.info(f"Connected to Cassandra keyspace '{CASSANDRA_KEYSPACE}'!")
            return cluster, session
        except Exception as e:
            logger.warning(f"Connection attempt {i+1} failed: {e}")
            if cluster:
                cluster.shutdown()
            time.sleep(5)
    logger.error("Failed to connect to Cassandra after multiple attempts.")
    raise ConnectionError("Could not connect to Cassandra")


def generate_test_data(session: Session):
    """
    Generate test data in Cassandra based on the designed schema.
    Uses direct inserts for efficiency.
    """
    logger.info("Generating test data...")

    user_ids = list(range(1, NUM_USERS + 1))
    created_conversations = set()

    insert_message_stmt = session.prepare("""
        INSERT INTO messages_by_conversation
        (conversation_id, message_time, message_id, sender_id, receiver_id, content)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    insert_user_convo_stmt = session.prepare("""
        INSERT INTO conversations_by_user
        (user_id, last_message_time, conversation_id, other_user_id, last_message_sender_id, last_message_content)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    insert_convo_lookup_stmt = session.prepare("""
        INSERT INTO conversation_by_users (user_a_id, user_b_id, conversation_id) VALUES (?, ?, ?)
    """)

    conversations_generated = 0
    attempts = 0
    max_attempts = NUM_CONVERSATIONS * 5 # Limit attempts to avoid infinite loops if NUM_USERS is small

    while conversations_generated < NUM_CONVERSATIONS and attempts < max_attempts:
        attempts += 1
        user1_id = random.choice(user_ids)
        user2_id = random.choice(user_ids)
        if user1_id == user2_id:
            continue

        user_a_id = min(user1_id, user2_id)
        user_b_id = max(user1_id, user2_id)
        convo_pair = (user_a_id, user_b_id)

        if convo_pair in created_conversations:
            continue

        conversation_id = uuid.uuid4()
        try:
            session.execute(insert_convo_lookup_stmt, (user_a_id, user_b_id, conversation_id), timeout=10.0)
            created_conversations.add(convo_pair)
            conversations_generated += 1
            logger.info(f"Created conversation {conversation_id} between users {user_a_id} and {user_b_id}")

        except Exception as e:
            logger.warning(f"Could not insert conversation lookup for ({user_a_id}, {user_b_id}): {e}. Skipping.")
            continue

        num_messages = random.randint(MIN_MESSAGES_PER_CONVERSATION, MAX_MESSAGES_PER_CONVERSATION)
        current_time = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30)) # Start sometime in the past month
        last_message_time = None
        last_message_content = ""
        last_message_sender_id = None

        batch = BatchStatement(consistency_level=WRITE_CONSISTENCY)
        messages_in_batch = 0
        MAX_BATCH_SIZE = 50 # Cassandra batch size recommendation

        for i in range(num_messages):
            sender = random.choice([user1_id, user2_id])
            receiver = user2_id if sender == user1_id else user1_id
            content = f"This is message {i+1}/{num_messages} in conversation {conversation_id} from {sender} to {receiver}."
            current_time += timedelta(seconds=random.randint(1, 3600))
            message_time = uuid_from_time(current_time)
            message_id = uuid.uuid4() # Unique ID per message

            batch.add(insert_message_stmt, (conversation_id, message_time, message_id, sender, receiver, content))
            messages_in_batch += 1

            last_message_time = message_time
            last_message_content = content[:200] # Snippet
            last_message_sender_id = sender

            if messages_in_batch >= MAX_BATCH_SIZE:
                 try:
                     session.execute(batch, timeout=20.0)
                     logger.debug(f"Executed batch of {messages_in_batch} messages for convo {conversation_id}")
                     batch = BatchStatement(consistency_level=WRITE_CONSISTENCY) # Reset batch
                     messages_in_batch = 0
                 except Exception as e:
                     logger.error(f"Error executing message batch for convo {conversation_id}: {e}")
                     # Decide how to handle: continue, break, raise? Let's continue for now.
                     batch = BatchStatement(consistency_level=WRITE_CONSISTENCY) # Reset batch
                     messages_in_batch = 0


        # Execute any remaining messages in the batch
        if messages_in_batch > 0:
            try:
                session.execute(batch, timeout=20.0)
                logger.debug(f"Executed final batch of {messages_in_batch} messages for convo {conversation_id}")
            except Exception as e:
                logger.error(f"Error executing final message batch for convo {conversation_id}: {e}")


        if last_message_time and last_message_sender_id is not None:
            batch_user_convo = BatchStatement(consistency_level=WRITE_CONSISTENCY)
             # For user 1
            batch_user_convo.add(insert_user_convo_stmt, (
                user1_id, last_message_time, conversation_id, user2_id, last_message_sender_id, last_message_content
            ))
             # For user 2
            batch_user_convo.add(insert_user_convo_stmt, (
                user2_id, last_message_time, conversation_id, user1_id, last_message_sender_id, last_message_content
            ))
            try:
                session.execute(batch_user_convo, timeout=10.0)
                logger.debug(f"Updated user_conversations for users {user1_id} and {user2_id} for convo {conversation_id}")
            except Exception as e:
                 logger.error(f"Error updating user_conversations for convo {conversation_id}: {e}")
        else:
             logger.warning(f"No messages generated for conversation {conversation_id}, skipping user_conversations update.")


    if conversations_generated < NUM_CONVERSATIONS:
         logger.warning(f"Only generated {conversations_generated}/{NUM_CONVERSATIONS} conversations due to potential user pair exhaustion or errors.")
    else:
        logger.info(f"Successfully generated {conversations_generated} conversations with messages.")

    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")


def main():
    """Main function to generate test data."""
    cluster = None
    session = None

    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()

        # Generate test data
        generate_test_data(session)

        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}", exc_info=True)
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main()