"""
Pancake Social Webhook Receiver
Listens for incoming Pancake Social webhook events (inbox, comments, messages)
and streams real-time data into dwh_pancake_chat_logs.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Any
from fastapi import FastAPI, Request, HTTPException
from loguru import logger

# Add parent directory for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.database import PostgreSQLHelper

POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://admin:QtdlxxrisA%40%21sfsgo%232024@10.10.10.224:5432/dev_staging_sci")

app = FastAPI(title="Pancake Webhook Receiver Service", version="1.0.0")
db_helper = PostgreSQLHelper(POSTGRES_URI)
TABLE_NAME = "dwh_pancake_chat_logs"


def ensure_table():
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        message_id VARCHAR(100) PRIMARY KEY,
        conversation_id VARCHAR(100),
        page_id VARCHAR(100),
        customer_id VARCHAR(100),
        customer_name VARCHAR(255),
        sender_type VARCHAR(50),
        message_text TEXT,
        payload_json JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    db_helper.execute(sql)


@app.on_event("startup")
def startup_event():
    ensure_table()
    logger.info(f"Webhook listener initialized. Target table: {TABLE_NAME}")


@app.post("/webhook/pancake")
async def handle_pancake_webhook(request: Request):
    """
    Endpoint for Pancake Social Webhook Events.
    Processes live conversation messages/comments and inserts into DWH.
    """
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"Invalid JSON payload received: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    event_type = payload.get("event") or payload.get("type") or "message"
    message_id = str(payload.get("message_id") or payload.get("id") or f"msg_{datetime.now().timestamp()}")
    conversation_id = str(payload.get("conversation_id") or payload.get("thread_id") or "")
    page_id = str(payload.get("page_id") or "")
    customer_id = str(payload.get("customer_id") or payload.get("from", {}).get("id") or "")
    customer_name = payload.get("customer_name") or payload.get("from", {}).get("name") or ""
    sender_type = payload.get("sender_type") or ("customer" if customer_id else "page")
    message_text = payload.get("message") or payload.get("text") or ""
    created_at = payload.get("timestamp") or datetime.now().isoformat()

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        message_id, conversation_id, page_id, customer_id, customer_name,
        sender_type, message_text, payload_json, created_at, synced_at
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
    )
    ON CONFLICT (message_id) DO UPDATE SET
        message_text = EXCLUDED.message_text,
        payload_json = EXCLUDED.payload_json,
        synced_at = CURRENT_TIMESTAMP;
    """

    try:
        cursor = db_helper.conn.cursor()
        cursor.execute(insert_sql, (
            message_id, conversation_id, page_id, customer_id, customer_name,
            sender_type, message_text, json.dumps(payload), created_at
        ))
        db_helper.conn.commit()
        logger.info(f"Ingested webhook chat message [{message_id}] for conversation [{conversation_id}]")
        return {"status": "success", "message_id": message_id}
    except Exception as e:
        logger.error(f"Error persisting webhook event to DB: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "pancake-webhook-listener"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
