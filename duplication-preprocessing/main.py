import json
import logging
import os
import sys
import base64
from flask import Flask, request

# =======================
# Configuration & Setup
# =======================
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("duplication-pre-process")

app = Flask(__name__)

# =======================
# Flask API Endpoint
# =======================

@app.route("/", methods=["POST"])
def handler():
    # 1. Receive the Pub/Sub envelope
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        logger.error("Invalid Pub/Sub message format.")
        return "Bad Request", 400

    pubsub_message = envelope["message"]
    
    try:
        # 2. Decode the Base64 data from the message
        if "data" in pubsub_message:
            raw_data = base64.b64decode(pubsub_message["data"]).decode("utf-8")
            payload = json.loads(raw_data)
            
            # 3. Print the decoded message for verification
            logger.info("--- Received Pub/Sub Message ---")
            logger.info(f"Payload: {json.dumps(payload, indent=2)}")
            logger.info("--------------------------------")

            # 4. Requirement Placeholder
            logger.info("!!! NEEDS TO CODE TO FOR DUPLICATION PREPROCESS !!!")

            # 5. Send 200 status to Pub/Sub to acknowledge receipt
            return "OK", 200

        else:
            logger.warning("Message received with no data field.")
            return "OK", 200

    except Exception as e:
        logger.error(f"[ERROR] Failed to decode message: {str(e)}")
        # We still return 200 if the data is garbage to avoid infinite retries
        return "OK", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)