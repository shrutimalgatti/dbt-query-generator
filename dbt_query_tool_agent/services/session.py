import uuid
from google.adk.sessions.in_memory_session_service import InMemorySessionService
#from google.adk.artifacts import InMemoryArtifactService
from google.adk.artifacts import GcsArtifactService

def create_session(app_name, user_id):
    session_service = InMemorySessionService()
    artifact_service = GcsArtifactService()
    session_id = f"session_{uuid.uuid4().hex[:8]}"
    session = session_service.create_session(
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
        state={}
    )
    return session_service, session_id, session,artifact_service