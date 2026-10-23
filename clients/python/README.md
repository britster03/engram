# engram-client (Python)

Official Python client for the Engram memory management API.

`ingest` creates an idempotency key once per method call and keeps it across
automatic HTTP retries. Supply `idempotency_key="your-request-id"` to retry the
same submission across calls. Reusing a key with different input returns 409;
separate calls without an explicit key create separate submissions.

`chat_completions` and `chat_completions_stream` accept optional
`client_message_id` for safe retries. Keep the same ID and request content when
retrying a completed request. The buffered response includes `run_id` and
`input_event_id`. A non-completed existing run returns 409; inspect its status
with `GET /api/v1/chat/runs/{run_id}` before sending another message.

```bash
pip install engram-client
```

```python
from engram_client import EngramClient

client = EngramClient(
    base_url="https://engram.example.com",
    api_key="your-key-here",
)

# Ingest a turn pair
client.ingest(
    session_id="sess-1",
    user="I just accepted a job at Meta.",
    assistant="Congratulations!",
)

# Query
r = client.query("Where does the user work?")
print(r.answer)
print(r.retrieval_metadata.cascade_depth_reached)
print(r.retrieval_metadata.token_usage.total_tokens)

# Streaming answers (Server-Sent Events)
for chunk in client.query_stream("What's my wife's birthday?"):
    print(chunk, end="", flush=True)

# Session-attached messages
sid = client.create_session()
client.send_message(sid, user="hi", assistant="hello")

# Admin operations (require an admin bearer token)
admin = EngramClient(base_url=..., api_key=admin_key)
tenant, api_key = admin.create_tenant("acme-corp", display_name="Acme Corp")
```

All methods retry with exponential backoff on 5xx / 429 responses. Set
`retries=0` to disable.

Names alone do not join entities across source events. To identify a known
person deliberately, pass `entity_bindings=[{"name": "John Smith",
"identity_key": "employee:42"}]` to `ingest()`. An existing tenant-local
`entity_id` can be supplied instead of `identity_key`.
