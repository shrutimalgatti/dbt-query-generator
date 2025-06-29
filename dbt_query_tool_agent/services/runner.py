from google.adk.memory.in_memory_memory_service import InMemoryMemoryService
from google.adk.runners import Runner
from google.adk.artifacts import GcsArtifactService

def create_runner(app_name, agent, session_service,gcsartifact):
    memory_service = InMemoryMemoryService()
    artifact_service = gcsartifact
    runner = Runner(
        app_name=app_name,
        agent=agent,
        session_service=session_service,
        memory_service=memory_service,
        artifact_service=artifact_service
    )
    return runner