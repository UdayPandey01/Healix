from fastapi import FastAPI, Request
from schemas import Alert
import uuid

app = FastAPI()

@app.post("/v1/alerts/ingest")
async def ingest_alert(alert: Alert, request: Request):

    authorization = request.headers.get("Authorization")

    if authorization is None:
        return {"error": "Authorization header missing"}

    token = authorization.replace("Bearer ", "")

    trace_id = "hlx_trc_" + str(uuid.uuid4())[:8]

    return {
        "status": "queued",
        "message": "Healix is investigating the incident.",
        "healix_trace_id": trace_id,
        "token_received": token
    }