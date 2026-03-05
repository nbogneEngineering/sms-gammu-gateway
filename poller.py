import hashlib
import logging
import os
import threading
import time

import requests

from support import encodeSms

logger = logging.getLogger(__name__)

# Configuration from environment variables
POLL_URL = os.getenv('POLL_URL', '')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '300'))  # default 5 minutes
DEST_NUMBER = os.getenv('DEST_NUMBER', '')
POLL_ENABLED = os.getenv('POLL_ENABLED', 'false').lower() == 'true'
POLL_AUTH_TOKEN = os.getenv('POLL_AUTH_TOKEN', '')


def _payload_id(payload):
    """Generate a unique fingerprint for a payload to detect duplicates."""
    raw = str(payload).encode('utf-8')
    return hashlib.sha256(raw).hexdigest()


def _build_sms_messages(text, numbers, smsc_location=1):
    """Wrap a text payload with SMS transmission headers and return encoded messages."""
    smsinfo = {
        "Class": -1,
        "Unicode": False,
        "Entries": [
            {
                "ID": "ConcatenatedTextLong",
                "Buffer": text,
            }
        ],
    }
    messages = []
    for number in numbers:
        number = number.strip()
        if not number:
            continue
        for message in encodeSms(smsinfo):
            message["SMSC"] = {'Location': smsc_location}
            message["Number"] = number
            messages.append(message)
    return messages


def _format_payload(payload):
    """Convert a payload (dict, list, or scalar) into a sendable SMS text string."""
    if isinstance(payload, dict):
        lines = []
        for key, value in payload.items():
            lines.append(f"{key}: {value}")
        return "\n".join(lines)
    if isinstance(payload, list):
        return "\n".join(str(item) for item in payload)
    return str(payload)


def fetch_payloads(url, auth_token=''):
    """Fetch payloads from the configured API endpoint. Returns a list of payloads."""
    headers = {'Accept': 'application/json'}
    if auth_token:
        headers['Authorization'] = f'Bearer {auth_token}'

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Normalise: if the API returns a single object, wrap it in a list
    if isinstance(data, dict):
        # If the response has a 'results', 'data', 'messages', or 'items' key, use that
        for key in ('results', 'data', 'messages', 'items', 'payloads'):
            if key in data and isinstance(data[key], list):
                return data[key]
        return [data]
    if isinstance(data, list):
        return data
    return [data]


def poll_and_send(machine, seen_ids):
    """Poll the API once, send any new payloads as SMS, return updated seen_ids set."""
    if not POLL_URL or not DEST_NUMBER:
        logger.warning(
            "POLL_URL or DEST_NUMBER not configured — skipping poll cycle")
        return seen_ids

    numbers = [n.strip() for n in DEST_NUMBER.split(',') if n.strip()]

    try:
        payloads = fetch_payloads(POLL_URL, POLL_AUTH_TOKEN)
    except requests.RequestException as exc:
        logger.error("Failed to fetch payloads from %s: %s", POLL_URL, exc)
        return seen_ids

    new_count = 0
    for payload in payloads:
        pid = _payload_id(payload)
        if pid in seen_ids:
            continue

        seen_ids.add(pid)
        text = _format_payload(payload)
        messages = _build_sms_messages(text, numbers)

        for message in messages:
            try:
                machine.SendSMS(message)
                logger.info("Sent SMS to %s: %s", message["Number"], text[:80])
            except Exception as exc:
                logger.error("Failed to send SMS to %s: %s",
                             message["Number"], exc)

        new_count += 1

    if new_count:
        logger.info("Processed %d new payload(s) from %s", new_count, POLL_URL)
    else:
        logger.debug("No new payloads at %s", POLL_URL)

    return seen_ids


def _poller_loop(machine):
    """Background loop that polls the API at the configured interval."""
    seen_ids = set()
    logger.info(
        "Poller started — URL=%s, interval=%ds, destination=%s",
        POLL_URL, POLL_INTERVAL, DEST_NUMBER,
    )
    while True:
        seen_ids = poll_and_send(machine, seen_ids)
        time.sleep(POLL_INTERVAL)


def start_poller(machine):
    """Launch the poller as a daemon thread (non-blocking)."""
    if not POLL_ENABLED:
        logger.info("Poller is disabled (set POLL_ENABLED=true to enable)")
        return None

    if not POLL_URL:
        logger.error(
            "POLL_ENABLED is true but POLL_URL is not set — poller not started")
        return None

    if not DEST_NUMBER:
        logger.error(
            "POLL_ENABLED is true but DEST_NUMBER is not set — poller not started")
        return None

    thread = threading.Thread(
        target=_poller_loop, args=(machine,), daemon=True)
    thread.start()
    return thread
