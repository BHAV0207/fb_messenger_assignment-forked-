from fastapi import HTTPException, status, Depends
import uuid
from typing import List

from app.models.cassandra_models import ConversationModel, MessageModel
from app.schemas.conversation import (
    ConversationResponse,
    PaginatedConversationResponse
)
from app.schemas.message import MessageResponse

UNKNOWN_TOTAL = -1

class ConversationController:
    """
    Controller for handling conversation operations.
    Connects API routes to ConversationModel methods.
    """


    async def get_user_conversations(
        self,
        user_id: int,
        page: int = 1,
        limit: int = 20
    ) -> PaginatedConversationResponse:
        """
        Get all conversations for a user with pagination.

        NOTE: Cassandra's standard pagination uses paging states, not page numbers.
        This implementation fetches a single 'page' based on the limit but doesn't
        support jumping to arbitrary page numbers efficiently. The 'page' parameter
        is largely ignored beyond the first page. A better API would use an
        opaque 'next_page_token' based on Cassandra's paging state.
        """
        if limit <= 0:
            limit = 20 

        try:
            conversations_data, next_paging_state = await ConversationModel.get_user_conversations(
                user_id=user_id,
                page_size=limit,
                paging_state=None
            )

            conversations = [ConversationResponse.model_validate(data) for data in conversations_data]


            return PaginatedConversationResponse(
                total=UNKNOWN_TOTAL,
                page=page,
                limit=limit,
                data=conversations


            )

        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch user conversations"
            )

    async def get_conversation(self, conversation_id: uuid.UUID) -> ConversationResponse:
        """
        Get a specific conversation by ID.

        NOTE: As noted in the model, getting full conversation details efficiently
        by ID alone might be hard depending on the chosen schema. This attempts
        to fetch it, but might rely on inefficient queries or return limited data
        if not designed for. The model implementation returns None.
        """
        try:
            conversation_data = await ConversationModel.get_conversation(conversation_id=conversation_id)

            if not conversation_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Conversation with ID {conversation_id} not found or cannot be efficiently retrieved by ID alone."
                )

            return ConversationResponse.model_validate(conversation_data)

        except HTTPException as http_exc:
            raise http_exc
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to fetch conversation {conversation_id}"
            )