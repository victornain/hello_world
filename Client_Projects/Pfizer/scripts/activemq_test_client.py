"""
AMQP 1.0 round-trip test client for the PharmaSuite exception ingestion queues.

Target: Internal validation broker, tcp://aznaampstalos1i:61646
(confirmed in PlanActiveMQ_a_SQL.md — plain tcp on 61646, NOT the ssl/61647
production endpoint).

What it does:
  1. Drains up to N pending messages from the inbound queue
     (PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST) and prints each payload.
  2. Publishes a sample ProcessRecordedException payload to the outbound queue
     (EXCEPTION_RECORD_EXTERNAL_RESPONSE) as a connectivity/round-trip check.

This is a connectivity test only, not the production connector described in
Phase 2/5 of PlanActiveMQ_a_SQL.md. The exact schema PRC expects on the
outbound queue is still an open question in that plan — the sample payload
below just reuses the known ProcessRecordedException shape so the test has a
well-formed body to send.

Requires: pip install python-qpid-proton

Credentials (only needed if the broker enforces SASL auth; leave unset to
connect anonymously):
  set ACTIVEMQ_USER=...
  set ACTIVEMQ_PASSWORD=...
"""

import argparse
import logging
import os
from pathlib import Path

from proton import Message, Timeout
from proton.utils import BlockingConnection

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("activemq_test_client")

BROKER_HOST = os.environ.get("ACTIVEMQ_HOST", "aznaampstalos1d")
BROKER_PORT = os.environ.get("ACTIVEMQ_PORT", "61646")
ACTIVEMQ_USER = "admin" # os.environ.get("admin")
ACTIVEMQ_PASSWORD = "admin" #os.environ.get("admin")

INBOUND_QUEUE = "PROCESS_EXCEPTION_RECORD_EXTERNALLY_REQUEST"
OUTBOUND_QUEUE = "EXCEPTION_RECORD_EXTERNAL_RESPONSE"

# Placeholder test payload only — the real outbound/response schema is unconfirmed (see Phase 5 open questions).
SAMPLE_RESPONSE_XML = """<ProcessRecordedException xmlns="http://www.mesa.org/xml/B2MML-V0600" xmlns:b2m="http://www.mesa.org/xml/B2MML-V0600-AllExtensions">
  <ApplicationArea>
    <Sender>
      <LogicalID>QMS</LogicalID>
    </Sender>
    <Receiver>
      <LogicalID>MES</LogicalID>
    </Receiver>
  </ApplicationArea>
  <DataArea>
    <PSUniqueExceptionID>TEST0000001 - 001</PSUniqueExceptionID>
    <OrderOrWorkflowIdentifier>TEST0000001</OrderOrWorkflowIdentifier>
    <ExceptionIdentifier>001</ExceptionIdentifier>
    <ExceptionDescription>Connectivity test message from activemq_test_client.py</ExceptionDescription>
  </DataArea>
</ProcessRecordedException>"""


def _broker_url() -> str:
    if ACTIVEMQ_USER and ACTIVEMQ_PASSWORD:
        return f"amqp://{ACTIVEMQ_USER}:{ACTIVEMQ_PASSWORD}@{BROKER_HOST}:{BROKER_PORT}"
    return f"amqp://{BROKER_HOST}:{BROKER_PORT}"


def consume_inbound(limit: int, timeout: float) -> list[str]:
    """Drain up to `limit` messages from the inbound queue, ack each, return raw bodies."""
    url = _broker_url()
    log.info("Connecting to %s to drain up to %d message(s) from %s", url, limit, INBOUND_QUEUE)
    conn = BlockingConnection(url, timeout=timeout)
    bodies: list[str] = []
    try:
        receiver = conn.create_receiver(INBOUND_QUEUE)
        for i in range(limit):
            try:
                delivery = receiver.receive(timeout=timeout)
            except Timeout:
                log.info("No more messages after %d received.", i)
                break
            bodies.append(delivery.body)
            receiver.accept()
            log.info("Received message %d:\n%s", i + 1, delivery.body)
    finally:
        conn.close()
    return bodies


def send_test_response(xml_payload: str) -> None:
    """Publish a single test message to the outbound queue."""
    url = _broker_url()
    log.info("Connecting to %s to send a test message to %s", url, OUTBOUND_QUEUE)
    conn = BlockingConnection(url)
    try:
        sender = conn.create_sender(OUTBOUND_QUEUE)
        sender.send(Message(body=xml_payload, content_type="text/xml"))
        log.info("Test message sent to %s", OUTBOUND_QUEUE)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consume-limit", type=int, default=10, help="Max messages to drain from the inbound queue")
    parser.add_argument("--timeout", type=float, default=5.0, help="Seconds to wait per receive before giving up")
    parser.add_argument("--skip-consume", action="store_true", help="Skip draining the inbound queue")
    parser.add_argument("--skip-send", action="store_true", help="Skip publishing the test message to the outbound queue")
    parser.add_argument("--payload-file", type=Path, default=None, help="XML file to send instead of the built-in sample")
    args = parser.parse_args()

    if not args.skip_consume:
        consume_inbound(args.consume_limit, args.timeout)

    if not args.skip_send:
        payload = args.payload_file.read_text(encoding="utf-8") if args.payload_file else SAMPLE_RESPONSE_XML
        send_test_response(payload)


if __name__ == "__main__":
    main()
