import uuid
from typing import Optional, Tuple
from google.adk.sessions.in_memory_session_service import InMemorySessionService
from google.adk.sessions.session import Session


async def create_session(
    app_name: str, user_id: str, session_id: Optional[str] = None
) -> Tuple[InMemorySessionService, str, Session]:
    """
    Creates a new session using InMemorySessionService.
    If session_id is provided, it uses it; otherwise, it generates a new one.
    """
    session_service = InMemorySessionService()
    if not session_id:
        session_id = f"session_{uuid.uuid4().hex[:8]}"

    session = await session_service.create_session(
        app_name=app_name, user_id=user_id, session_id=session_id
    )
    return session_service, session.id, session