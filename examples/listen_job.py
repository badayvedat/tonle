"""Example customer listener for the gateway + tonle flow."""

from __future__ import annotations

import os

import httpx

from tonle import Stream, StreamTrimmedError

GATEWAY_BASE_URL = os.getenv("GATEWAY_BASE_URL", "http://127.0.0.1:8010")
DEMO_TENANT = os.getenv("DEMO_TENANT", "t1")
DEMO_CUSTOMER_ID = os.getenv("DEMO_CUSTOMER_ID", "customer-123")
JOB_ID = os.getenv("JOB_ID", "job-1")


def main() -> None:
    headers = {
        "X-Demo-Tenant": DEMO_TENANT,
        "X-Demo-Customer-Id": DEMO_CUSTOMER_ID,
    }

    with httpx.Client(base_url=GATEWAY_BASE_URL, timeout=30.0) as client:
        snapshot = client.get(f"/tenants/{DEMO_TENANT}/jobs/{JOB_ID}", headers=headers)
        snapshot.raise_for_status()
        print("snapshot", snapshot.json())

        ticket_response = client.post(
            f"/tenants/{DEMO_TENANT}/jobs/{JOB_ID}/stream-ticket",
            headers=headers,
        )
        ticket_response.raise_for_status()
        ticket = ticket_response.json()

    print("listening", ticket["base_url"], ticket["stream_id"])
    stream = Stream(
        ticket["stream_id"],
        base_url=ticket["base_url"],
        token=ticket["token"],
    )

    try:
        for event in stream.events():
            print(event["id"], event["data"])
    except StreamTrimmedError:
        print("stream trimmed; fetch the latest snapshot and request a new ticket")
    finally:
        stream.close()


if __name__ == "__main__":
    main()
