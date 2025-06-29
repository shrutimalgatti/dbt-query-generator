
import vertexai
from vertexai import agent_engines
remote_app = vertexai.agent_engines.get('projects/32986790953/locations/us-central1/reasoningEngines/3821101973684355072')
remote_session = remote_app.create_session(user_id="u_456")
for event in remote_session.stream_query (
    user_id="u_456",
    session_id=remote_session.id,
    message="Source database is teradata, source path is gs://gemini-validator/agent/td/ and target path is gs://gemini-validator/agent/bq/"
    ):
        print(event)

