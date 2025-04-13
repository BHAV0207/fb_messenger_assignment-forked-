

import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import logging

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BoundStatement
from cassandra.util import uuid_from_time

from app.db.cassandra_client import cassandra_client
# Schemas are not strictly needed in the model file, but can be useful for reference
# from app.schemas.message import MessageResponse
# from app.schemas.conversation import ConversationResponse

logger = logging.getLogger(__name__)

WRITE_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM
READ_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM

class MessageModel:
    """
    Message model for interacting with the messages table (using synchronous execute).
    """

    @staticmethod
    async def _get_or_create_conversation_id(user1_id: int, user2_id: int) -> uuid.UUID:
        """
        Finds the existing conversation ID between two users or creates a new one.
        Ensures user IDs are ordered consistently. (Synchronous DB calls)
        """
        if user1_id == user2_id:
             raise ValueError("Cannot create conversation with oneself")
        user_a_id = min(user1_id, user2_id)
        user_b_id = max(user1_id, user2_id)

        session = cassandra_client.get_session()

        # 1. Check if conversation exists (using synchronous execute)
        select_query = """
        SELECT conversation_id FROM conversation_by_users
        WHERE user_a_id = %s AND user_b_id = %s LIMIT 1
        """
        select_statement = SimpleStatement(select_query, consistency_level=READ_CONSISTENCY)

        # Use synchronous execute()
        result = session.execute(select_statement, (user_a_id, user_b_id))
        row = result.one()

        if row:
            return row['conversation_id']
        else:
            # 2. Create new conversation ID if not found (using synchronous execute)
            new_conversation_id = uuid.uuid4()
            insert_query = """
            INSERT INTO conversation_by_users (user_a_id, user_b_id, conversation_id)
            VALUES (%s, %s, %s)
            """
            insert_statement = SimpleStatement(insert_query, consistency_level=WRITE_CONSISTENCY)

            # Use synchronous execute()
            session.execute(insert_statement, (user_a_id, user_b_id, new_conversation_id))
            logger.info(f"Created new conversation between {user_a_id} and {user_b_id} with ID: {new_conversation_id}")
            return new_conversation_id

    @staticmethod
    async def create_message(
        sender_id: int, receiver_id: int, content: str
    ) -> Tuple[Optional[uuid.UUID], Optional[uuid.UUID], Optional[uuid.UUID]]: # Added message_id to return tuple
        """
        Creates a new message, saves it, and updates conversation metadata. (Synchronous DB calls)

        Returns:
            A tuple containing (conversation_id, message_timeuuid, message_id) or (None, None, None) on failure.
        """
        session = cassandra_client.get_session()
        try:
            # 1. Get or create the conversation ID (this method is now internally synchronous with DB)
            conversation_id = await MessageModel._get_or_create_conversation_id(sender_id, receiver_id)

            # 2. Generate message details
            message_time = uuid_from_time(datetime.utcnow())
            message_id = uuid.uuid4() # Keep the generated message_id

            # 3. Insert the message (using synchronous execute)
            insert_message_query = """
            INSERT INTO messages_by_conversation
            (conversation_id, message_time, message_id, sender_id, receiver_id, content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            message_statement = SimpleStatement(insert_message_query, consistency_level=WRITE_CONSISTENCY)
            session.execute(
                message_statement, (conversation_id, message_time, message_id, sender_id, receiver_id, content)
            )

            # 4. Update conversations_by_user (using synchronous execute)
            update_conversation_query = """
            INSERT INTO conversations_by_user
            (user_id, last_message_time, conversation_id, other_user_id, last_message_sender_id, last_message_content)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            conv_statement = SimpleStatement(update_conversation_query, consistency_level=WRITE_CONSISTENCY)

            # Update for sender
            session.execute(
                 conv_statement, (sender_id, message_time, conversation_id, receiver_id, sender_id, content[:200])
            )
            # Update for receiver
            session.execute(
                 conv_statement, (receiver_id, message_time, conversation_id, sender_id, sender_id, content[:200])
            )

            logger.info(f"Message {message_id} created in conversation {conversation_id}")
            # Return the generated message_id as well
            return conversation_id, message_time, message_id

        except Exception as e:
            logger.error(f"Failed to create message: {str(e)}", exc_info=True)
            return None, None, None # Indicate failure


    @staticmethod
    async def get_conversation_messages(
        conversation_id: uuid.UUID,
        page_size: int = 20,
        paging_state: Optional[bytes] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
        """
        Get messages for a conversation with pagination. (Synchronous DB calls)
        """
        session = cassandra_client.get_session()
        query = """
        SELECT conversation_id, message_time, message_id, sender_id, receiver_id, content
        FROM messages_by_conversation
        WHERE conversation_id = %s
        """
        # fetch_size is used by the driver for paging even with synchronous execute
        statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

        try:
            # Use synchronous execute(), passing the paging state if it exists
            results = session.execute(statement, (conversation_id,), paging_state=paging_state)

            messages = list(results.current_rows)
            next_paging_state = results.paging_state # Paging state still works

            for msg in messages:
                if msg.get('message_time'):
                    msg_timestamp = uuid.UUID(bytes=msg['message_time'].bytes).time
                    msg['created_at'] = datetime.utcfromtimestamp(msg_timestamp / 1e9)
                else:
                     msg['created_at'] = None
                msg['id'] = msg.get('message_id')

            return messages, next_paging_state

        except Exception as e:
            logger.error(f"Failed to get messages for conversation {conversation_id}: {str(e)}", exc_info=True)
            return [], None


    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: uuid.UUID,
        before_timestamp: datetime,
        page_size: int = 20,
        paging_state: Optional[bytes] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
        """
        Get messages before a timestamp with pagination. (Synchronous DB calls)
        """
        session = cassandra_client.get_session()
        before_timeuuid = uuid_from_time(before_timestamp)

        query = """
        SELECT conversation_id, message_time, message_id, sender_id, receiver_id, content
        FROM messages_by_conversation
        WHERE conversation_id = %s AND message_time < %s
        """
        statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

        try:
            # Use synchronous execute()
            results = session.execute(statement, (conversation_id, before_timeuuid), paging_state=paging_state)

            messages = list(results.current_rows)
            next_paging_state = results.paging_state

            for msg in messages:
                if msg.get('message_time'):
                    msg_timestamp = uuid.UUID(bytes=msg['message_time'].bytes).time
                    msg['created_at'] = datetime.utcfromtimestamp(msg_timestamp / 1e9)
                else:
                     msg['created_at'] = None
                msg['id'] = msg.get('message_id')

            return messages, next_paging_state

        except Exception as e:
            logger.error(f"Failed to get messages before timestamp for conv {conversation_id}: {str(e)}", exc_info=True)
            return [], None


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables. (Synchronous DB calls)
    """

    @staticmethod
    async def get_user_conversations(
        user_id: int,
        page_size: int = 20,
        paging_state: Optional[bytes] = None
    ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
        """
        Get conversations for a user with pagination, sorted by most recent. (Synchronous DB calls)
        """
        session = cassandra_client.get_session()
        query = """
        SELECT user_id, last_message_time, conversation_id, other_user_id,
               last_message_sender_id, last_message_content
        FROM conversations_by_user
        WHERE user_id = %s
        """
        statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

        try:
            # Use synchronous execute()
            results = session.execute(statement, (user_id,), paging_state=paging_state)

            conversations = list(results.current_rows)
            next_paging_state = results.paging_state

            formatted_convos = []
            for convo in conversations:
                 if convo.get('last_message_time'):
                     last_msg_timestamp_ns = uuid.UUID(bytes=convo['last_message_time'].bytes).time
                     last_msg_dt = datetime.utcfromtimestamp(last_msg_timestamp_ns / 1e9)
                 else:
                     last_msg_dt = None # Handle case where time might be missing

                 formatted_convos.append({
                     "id": convo.get('conversation_id'),
                     "user1_id": user_id,
                     "user2_id": convo.get('other_user_id'),
                     "last_message_at": last_msg_dt,
                     "last_message_content": convo.get('last_message_content'),
                 })

            return formatted_convos, next_paging_state

        except Exception as e:
            logger.error(f"Failed to get conversations for user {user_id}: {str(e)}", exc_info=True)
            return [], None


    @staticmethod
    async def get_conversation(conversation_id: uuid.UUID) -> Optional[Dict[str, Any]]:
        """
        Get a specific conversation's metadata. (Remains largely unsupported by schema efficiently)
        """
        logger.warning("get_conversation by ID alone is not efficiently supported by the current schema design. Obtain conversation details via get_user_conversations.")
        return None # Keep returning None as schema doesn't support this well


    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Optional[uuid.UUID]:
        """
        Gets the conversation ID between two users, creating the entry if it doesn't exist.
        (Uses the modified _get_or_create_conversation_id)
        """
        try:
            # Delegate to the internal helper method (which now uses sync execute)
            conversation_id = await MessageModel._get_or_create_conversation_id(user1_id, user2_id)
            return conversation_id
        except Exception as e:
            logger.error(f"Failed in create_or_get_conversation between {user1_id} and {user2_id}: {str(e)}", exc_info=True)
            return None
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------
# import uuid
# from datetime import datetime
# from typing import List, Dict, Any, Optional, Tuple
# import logging

# from cassandra import ConsistencyLevel
# from cassandra.query import SimpleStatement, BoundStatement
# from cassandra.util import uuid_from_time # To generate TimeUUIDs
# from cassandra.cqlengine.connection import execute # Use execute for async behavior with futures

# from app.db.cassandra_client import cassandra_client
# from app.schemas.message import MessageResponse # Import necessary schemas
# from app.schemas.conversation import ConversationResponse # Import necessary schemas

# logger = logging.getLogger(__name__)

# # Define consistency levels (adjust as needed for your application's requirements)
# WRITE_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM
# READ_CONSISTENCY = ConsistencyLevel.LOCAL_QUORUM

# class MessageModel:
#     """
#     Message model for interacting with the messages table.
#     """

#     @staticmethod
#     async def _get_or_create_conversation_id(user1_id: int, user2_id: int) -> uuid.UUID:
#         """
#         Finds the existing conversation ID between two users or creates a new one.
#         Ensures user IDs are ordered consistently.
#         """
#         # Ensure consistent ordering of user IDs
#         if user1_id == user2_id:
#              raise ValueError("Cannot create conversation with oneself") # Prevent self-conversation
#         user_a_id = min(user1_id, user2_id)
#         user_b_id = max(user1_id, user2_id)

#         session = cassandra_client.get_session()

#         # 1. Check if conversation exists
#         select_query = """
#         SELECT conversation_id FROM conversation_by_users
#         WHERE user_a_id = %s AND user_b_id = %s LIMIT 1
#         """
#         select_statement = SimpleStatement(select_query, consistency_level=READ_CONSISTENCY)

#         # Use execute_async for non-blocking I/O, then await the result
#         future = session.execute_async(select_statement, (user_a_id, user_b_id))
#         result = await future # Await the async execution

#         row = result.one() # Fetch one row

#         if row:
#             return row['conversation_id']
#         else:
#             # 2. Create new conversation ID if not found
#             new_conversation_id = uuid.uuid4() # Generate a standard UUID v4
#             insert_query = """
#             INSERT INTO conversation_by_users (user_a_id, user_b_id, conversation_id)
#             VALUES (%s, %s, %s)
#             """
#             # Using LOCAL_QUORUM for write consistency
#             insert_statement = SimpleStatement(insert_query, consistency_level=WRITE_CONSISTENCY)

#             future = session.execute_async(insert_statement, (user_a_id, user_b_id, new_conversation_id))
#             await future # Await the async execution
#             logger.info(f"Created new conversation between {user_a_id} and {user_b_id} with ID: {new_conversation_id}")
#             return new_conversation_id

#     @staticmethod
#     async def create_message(
#         sender_id: int, receiver_id: int, content: str
#     ) -> Tuple[Optional[uuid.UUID], Optional[uuid.UUID]]:
#         """
#         Creates a new message, saves it, and updates conversation metadata.

#         Args:
#             sender_id: ID of the message sender.
#             receiver_id: ID of the message receiver.
#             content: Text content of the message.

#         Returns:
#             A tuple containing (conversation_id, message_timeuuid) or (None, None) on failure.
#         """
#         session = cassandra_client.get_session()
#         try:
#             # 1. Get or create the conversation ID
#             conversation_id = await MessageModel._get_or_create_conversation_id(sender_id, receiver_id)

#             # 2. Generate message details
#             message_time = uuid_from_time(datetime.utcnow()) # Generate a TimeUUID based on current time
#             message_id = uuid.uuid4() # Generate a standard UUID v4 as a distinct message identifier

#             # 3. Insert the message into messages_by_conversation table
#             insert_message_query = """
#             INSERT INTO messages_by_conversation
#             (conversation_id, message_time, message_id, sender_id, receiver_id, content)
#             VALUES (%s, %s, %s, %s, %s, %s)
#             """
#             message_statement = SimpleStatement(insert_message_query, consistency_level=WRITE_CONSISTENCY)
#             future_msg = session.execute_async(
#                 message_statement, (conversation_id, message_time, message_id, sender_id, receiver_id, content)
#             )

#             # 4. Update the conversations_by_user table for BOTH sender and receiver
#             # This keeps the conversation list sorted by the latest message time.
#             # Use UPSERT logic (INSERT acts as UPSERT on primary key)
#             update_conversation_query = """
#             INSERT INTO conversations_by_user
#             (user_id, last_message_time, conversation_id, other_user_id, last_message_sender_id, last_message_content)
#             VALUES (%s, %s, %s, %s, %s, %s)
#             """
#             conv_statement = SimpleStatement(update_conversation_query, consistency_level=WRITE_CONSISTENCY)

#             # Update for sender
#             future_conv_sender = session.execute_async(
#                  conv_statement, (sender_id, message_time, conversation_id, receiver_id, sender_id, content[:200]) # Limit content snippet
#             )
#             # Update for receiver
#             future_conv_receiver = session.execute_async(
#                  conv_statement, (receiver_id, message_time, conversation_id, sender_id, sender_id, content[:200]) # Limit content snippet
#             )

#             # Await all async operations
#             await future_msg
#             await future_conv_sender
#             await future_conv_receiver

#             logger.info(f"Message {message_id} created in conversation {conversation_id}")
#             return conversation_id, message_time

#         except Exception as e:
#             logger.error(f"Failed to create message: {str(e)}", exc_info=True)
#             return None, None # Indicate failure


#     @staticmethod
#     async def get_conversation_messages(
#         conversation_id: uuid.UUID,
#         page_size: int = 20,
#         paging_state: Optional[bytes] = None
#     ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
#         """
#         Get messages for a conversation with pagination.

#         Args:
#             conversation_id: ID of the conversation.
#             page_size: Number of messages per page.
#             paging_state: Opaque state from Cassandra for fetching the next page.

#         Returns:
#             A tuple containing (list of messages as dicts, next paging state).
#         """
#         session = cassandra_client.get_session()
#         query = """
#         SELECT conversation_id, message_time, message_id, sender_id, receiver_id, content
#         FROM messages_by_conversation
#         WHERE conversation_id = %s
#         """
#         statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

#         try:
#             # Execute asynchronously, passing the paging state if it exists
#             future = session.execute_async(statement, (conversation_id,), paging_state=paging_state)
#             results = await future # Await the ResultSetFuture

#             messages = list(results.current_rows) # Get rows from the current page
#             next_paging_state = results.paging_state # Get the state for the *next* page

#             # Convert message_time (TimeUUID) to datetime string for response
#             for msg in messages:
#                 if msg.get('message_time'):
#                     # Extract timestamp from TimeUUID
#                     msg_timestamp = uuid.UUID(bytes=msg['message_time'].bytes).time
#                     # Convert nanoseconds since epoch to datetime object
#                     msg['created_at'] = datetime.utcfromtimestamp(msg_timestamp / 1e9) # Convert ns to s
#                 else:
#                      msg['created_at'] = None # Or handle as error
#                 # Map fields to match MessageResponse if needed (though controller often does this)
#                 msg['id'] = msg.get('message_id') # Example mapping


#             return messages, next_paging_state

#         except Exception as e:
#             logger.error(f"Failed to get messages for conversation {conversation_id}: {str(e)}", exc_info=True)
#             return [], None


#     @staticmethod
#     async def get_messages_before_timestamp(
#         conversation_id: uuid.UUID,
#         before_timestamp: datetime, # Use datetime object directly
#         page_size: int = 20,
#         paging_state: Optional[bytes] = None
#     ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
#         """
#         Get messages before a timestamp with pagination.

#         Args:
#             conversation_id: ID of the conversation.
#             before_timestamp: Get messages strictly before this timestamp (exclusive).
#             page_size: Number of messages per page.
#             paging_state: Opaque state from Cassandra for fetching the next page.

#         Returns:
#             A tuple containing (list of messages as dicts, next paging state).
#         """
#         session = cassandra_client.get_session()

#         # Generate a TimeUUID corresponding to the start of the provided timestamp
#         # Note: Cassandra's < comparison on timeuuid works correctly for time ordering
#         before_timeuuid = uuid_from_time(before_timestamp)

#         query = """
#         SELECT conversation_id, message_time, message_id, sender_id, receiver_id, content
#         FROM messages_by_conversation
#         WHERE conversation_id = %s AND message_time < %s
#         """ # Using '<' for messages *before* the timestamp
#         statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

#         try:
#             # Execute asynchronously
#             future = session.execute_async(statement, (conversation_id, before_timeuuid), paging_state=paging_state)
#             results = await future

#             messages = list(results.current_rows)
#             next_paging_state = results.paging_state

#              # Convert message_time (TimeUUID) to datetime string for response
#             for msg in messages:
#                 if msg.get('message_time'):
#                     msg_timestamp = uuid.UUID(bytes=msg['message_time'].bytes).time
#                     msg['created_at'] = datetime.utcfromtimestamp(msg_timestamp / 1e9)
#                 else:
#                      msg['created_at'] = None
#                 msg['id'] = msg.get('message_id')


#             return messages, next_paging_state

#         except Exception as e:
#             logger.error(f"Failed to get messages before timestamp for conv {conversation_id}: {str(e)}", exc_info=True)
#             return [], None


# class ConversationModel:
#     """
#     Conversation model for interacting with the conversations-related tables.
#     """

#     @staticmethod
#     async def get_user_conversations(
#         user_id: int,
#         page_size: int = 20,
#         paging_state: Optional[bytes] = None
#     ) -> Tuple[List[Dict[str, Any]], Optional[bytes]]:
#         """
#         Get conversations for a user with pagination, sorted by most recent.

#         Args:
#             user_id: ID of the user whose conversations to fetch.
#             page_size: Number of conversations per page.
#             paging_state: Opaque state for fetching the next page.

#         Returns:
#             A tuple containing (list of conversation dicts, next paging state).
#         """
#         session = cassandra_client.get_session()
#         query = """
#         SELECT user_id, last_message_time, conversation_id, other_user_id,
#                last_message_sender_id, last_message_content
#         FROM conversations_by_user
#         WHERE user_id = %s
#         """
#         statement = SimpleStatement(query, fetch_size=page_size, consistency_level=READ_CONSISTENCY)

#         try:
#             future = session.execute_async(statement, (user_id,), paging_state=paging_state)
#             results = await future

#             conversations = list(results.current_rows)
#             next_paging_state = results.paging_state

#             # Format results slightly to better match ConversationResponse expectations
#             formatted_convos = []
#             for convo in conversations:
#                  # Extract timestamp from TimeUUID
#                  last_msg_timestamp_ns = uuid.UUID(bytes=convo['last_message_time'].bytes).time
#                  last_msg_dt = datetime.utcfromtimestamp(last_msg_timestamp_ns / 1e9)

#                  formatted_convos.append({
#                      "id": convo['conversation_id'], # Use conversation_id as the primary ID
#                      "user1_id": user_id, # The user we queried for
#                      "user2_id": convo['other_user_id'], # The other participant
#                      "last_message_at": last_msg_dt,
#                      "last_message_content": convo.get('last_message_content'),
#                      # Add last_message_sender_id if needed by schema/frontend
#                  })


#             return formatted_convos, next_paging_state

#         except Exception as e:
#             logger.error(f"Failed to get conversations for user {user_id}: {str(e)}", exc_info=True)
#             return [], None


#     @staticmethod
#     async def get_conversation(conversation_id: uuid.UUID) -> Optional[Dict[str, Any]]:
#         """
#         Get a specific conversation's metadata.
#         Note: This might not be strictly necessary if `get_user_conversations`
#               and the initial message creation handle all needed info.
#               This implementation fetches details from the `conversation_by_users` table.
#               It requires knowing *one* participant ID to query that table effectively,
#               or requires a full table scan (not recommended) or a secondary index.
#               Let's assume we *don't* implement this via `conversation_by_users` directly
#               as it's hard without knowing one user. The info is typically derived when
#               fetching user conversations. Returning NotImplemented for now as the primary
#               access pattern is via `get_user_conversations`.
#               If absolutely needed, you might query `messages_by_conversation` LIMIT 1
#               to get participant IDs, but that's inefficient.

#          Args:
#              conversation_id: ID of the conversation to fetch.

#          Returns:
#              Conversation details as a dict, or None if not found/implementable efficiently.
#          """
#         # This method is difficult to implement efficiently with the current schema
#         # without knowing at least one user ID involved. The necessary data is
#         # usually retrieved via `get_user_conversations`.
#         logger.warning("get_conversation by ID alone is not efficiently supported by the current schema design. Obtain conversation details via get_user_conversations.")
#         # raise NotImplementedError("This method is difficult to implement efficiently with the current schema.")
#         return None # Or return basic info if you add a dedicated 'conversations' metadata table


#     @staticmethod
#     async def create_or_get_conversation(user1_id: int, user2_id: int) -> Optional[uuid.UUID]:
#         """
#         Gets the conversation ID between two users, creating the entry in
#         `conversation_by_users` if it doesn't exist.

#         Args:
#             user1_id: ID of the first user.
#             user2_id: ID of the second user.

#         Returns:
#             The conversation_id (UUID) or None on failure.
#         """
#         try:
#             # Delegate to the internal helper method used by create_message
#             conversation_id = await MessageModel._get_or_create_conversation_id(user1_id, user2_id)
#             return conversation_id
#         except Exception as e:
#             logger.error(f"Failed in create_or_get_conversation between {user1_id} and {user2_id}: {str(e)}", exc_info=True)
#             return None