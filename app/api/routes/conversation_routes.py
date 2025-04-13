from fastapi import APIRouter, Depends, Query, Path

from app.controllers.conversation_controller import ConversationController
from app.schemas.conversation import (
    ConversationResponse,
    PaginatedConversationResponse
)
from uuid import UUID

router = APIRouter(prefix="/api/conversations", tags=["Conversations"])

@router.get("/user/{user_id}", response_model=PaginatedConversationResponse)
async def get_user_conversations(
    user_id: int = Path(..., description="ID of the user"),
    page: int = Query(1, description="Page number"),
    limit: int = Query(20, description="Number of conversations per page"),
    conversation_controller: ConversationController = Depends()
) -> PaginatedConversationResponse:
    """
    Get all conversations for a user with pagination
    """
    return await conversation_controller.get_user_conversations(
        user_id=user_id,
        page=page,
        limit=limit
    )

@router.get("/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: UUID = Path(..., description="ID of the conversation"),
    conversation_controller: ConversationController = Depends()
) -> ConversationResponse:
    """
    Get a specific conversation by ID
    """
    return await conversation_controller.get_conversation(conversation_id=conversation_id)