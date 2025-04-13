from fastapi import HTTPException, status, Depends, Body
from typing import Optional, List
from datetime import datetime
import uuid

from app.models.cassandra_models import MessageModel
from app.schemas.message import (
    MessageCreate,
    MessageResponse,
    PaginatedMessageResponse
)

UNKNOWN_TOTAL = -1

class MessageController:
    """
    Controller for handling message operations.
    Connects API routes to MessageModel methods.
    """

    async def send_message(self, message_data: MessageCreate) -> MessageResponse:
        """
        Send a message from one user to another.
        """
        try:
            conversation_id, message_timeuuid, message_id = await MessageModel.create_message(
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                content=message_data.content
            )

            if conversation_id is None or message_timeuuid is None or message_id is None:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to send message due to internal error."
                )

            msg_timestamp_ns = message_timeuuid.time
            created_at_dt = datetime.utcfromtimestamp(msg_timestamp_ns / 1e9)


            return MessageResponse(
                id=message_id, # Use the actual message_id returned by the model
                sender_id=message_data.sender_id,
                receiver_id=message_data.receiver_id,
                content=message_data.content,
                created_at=created_at_dt,
                conversation_id=conversation_id
            )

        except ValueError as ve: # Catch specific errors like self-conversation
             raise HTTPException(
                 status_code=status.HTTP_400_BAD_REQUEST,
                 detail=str(ve)
             )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to send message"
            )

    async def get_conversation_messages(
        self,
        conversation_id: uuid.UUID,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get all messages in a conversation with pagination.

        NOTE: See pagination note in ConversationController.get_user_conversations.
        This implementation primarily uses 'limit' and doesn't support arbitrary 'page'.
        """
        if limit <= 0:
             limit = 20

        try:
            messages_data, next_paging_state = await MessageModel.get_conversation_messages(
                conversation_id=conversation_id,
                page_size=limit,
                paging_state=None # For first page
            )

            messages = [MessageResponse.model_validate(data) for data in messages_data]

            return PaginatedMessageResponse(
                total=UNKNOWN_TOTAL,
                page=page,
                limit=limit,
                data=messages
                # Ideally include next_paging_state as next_page_token
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages for conversation {conversation_id}"
            )

    async def get_messages_before_timestamp(
        self,
        conversation_id: uuid.UUID,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedMessageResponse:
        """
        Get messages in a conversation before a specific timestamp with pagination.

        NOTE: See pagination note in ConversationController.get_user_conversations.
        This implementation primarily uses 'limit' and doesn't support arbitrary 'page'.
        """
        if limit <= 0:
             limit = 20

        try:
             # Fetch the first page only
            messages_data, next_paging_state = await MessageModel.get_messages_before_timestamp(
                conversation_id=conversation_id,
                before_timestamp=before_timestamp,
                page_size=limit,
                paging_state=None # For first page
            )

            messages = [MessageResponse.model_validate(data) for data in messages_data]

            return PaginatedMessageResponse(
                total=UNKNOWN_TOTAL,
                page=page,
                limit=limit,
                data=messages
                 # Ideally include next_paging_state as next_page_token
            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch messages before timestamp for conversation {conversation_id}"
            )