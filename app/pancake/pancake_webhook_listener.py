"""FastAPI receiver for Pancake messaging webhooks."""

import json
import os
import sys
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from loguru import logger

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper


app = FastAPI(title="Pancake Webhook Receiver Service", version="1.1.0")
TABLE_NAME = "dwh_pancake_chat_logs"
db_helper: Optional[PostgreSQLHelper] = None


def ensure_table(db: PostgreSQLHelper) -> None:
    db.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            message_id VARCHAR(100) PRIMARY KEY,
            conversation_id VARCHAR(255),
            page_id VARCHAR(100),
            customer_id VARCHAR(100),
            customer_name VARCHAR(255),
            sender_type VARCHAR(50),
            event_type VARCHAR(50),
            message_text TEXT,
            payload_json JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        ALTER TABLE {TABLE_NAME}
            ADD COLUMN IF NOT EXISTS event_type VARCHAR(50);
        """
    )


@app.on_event("startup")
def startup_event() -> None:
    global db_helper
    postgres_uri = os.getenv("POSTGRES_URI")
    if not postgres_uri:
        raise RuntimeError("POSTGRES_URI is required")
    db_helper = PostgreSQLHelper(postgres_uri)
    ensure_table(db_helper)
    logger.info(f"Webhook listener initialized. Target table: {TABLE_NAME}")


@app.on_event("shutdown")
def shutdown_event() -> None:
    global db_helper
    if db_helper:
        db_helper.close()
        db_helper = None


@app.post("/webhook/pancake")
async def handle_pancake_webhook(request: Request):
    """Persist the documented nested `event_type/data/message` payload."""
    try:
        payload = await request.json()
    except Exception as exc:
        logger.error(f"Invalid JSON payload received: {exc}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    event_type = str(payload.get("event_type") or payload.get("event") or "")
    if event_type != "messaging":
        return {"status": "accepted", "event_type": event_type, "persisted": False}

    data = payload.get("data") or {}
    message = data.get("message") or payload
    conversation = data.get("conversation") or {}
    if not isinstance(message, dict) or not isinstance(conversation, dict):
        raise HTTPException(status_code=400, detail="Invalid Pancake webhook shape")

    message_id = str(message.get("id") or message.get("message_id") or "").strip()
    if not message_id:
        raise HTTPException(status_code=400, detail="Missing message id")

    sender = message.get("from") or {}
    if not isinstance(sender, dict):
        sender = {}
    page_id = str(message.get("page_id") or payload.get("page_id") or "")
    customer_id = str(
        sender.get("page_customer_id")
        or conversation.get("customer_id")
        or sender.get("id")
        or ""
    )
    sender_type = "page" if str(sender.get("id") or "") == page_id else "customer"
    created_at = message.get("inserted_at") or message.get("timestamp")

    insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            message_id, conversation_id, page_id, customer_id, customer_name,
            sender_type, event_type, message_text, payload_json,
            created_at, synced_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            COALESCE(%s::timestamp, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP
        )
        ON CONFLICT (message_id) DO UPDATE SET
            conversation_id = EXCLUDED.conversation_id,
            page_id = EXCLUDED.page_id,
            customer_id = EXCLUDED.customer_id,
            customer_name = EXCLUDED.customer_name,
            sender_type = EXCLUDED.sender_type,
            event_type = EXCLUDED.event_type,
            message_text = EXCLUDED.message_text,
            payload_json = EXCLUDED.payload_json,
            created_at = EXCLUDED.created_at,
            synced_at = CURRENT_TIMESTAMP;
    """

    if db_helper is None:
        raise HTTPException(status_code=503, detail="Database is not ready")
    try:
        with db_helper.conn.cursor() as cursor:
            cursor.execute(
                insert_sql,
                (
                    message_id,
                    str(message.get("conversation_id") or conversation.get("id") or ""),
                    page_id,
                    customer_id,
                    sender.get("name") or "",
                    sender_type,
                    event_type,
                    message.get("message") or message.get("original_message") or "",
                    json.dumps(payload, ensure_ascii=False),
                    created_at,
                ),
            )
        db_helper.conn.commit()
    except Exception as exc:
        db_helper.conn.rollback()
        logger.error(f"Error persisting Pancake webhook event: {exc}")
        raise HTTPException(status_code=500, detail="Could not persist event") from exc

    return {"status": "success", "message_id": message_id}


@app.get("/health")
def health_check():
    return {
        "status": "healthy" if db_helper is not None else "starting",
        "service": "pancake-webhook-listener",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
