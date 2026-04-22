# engram-client (Python)

Official Python client for the Engram memory management API.

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
