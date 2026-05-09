"""Example internal worker publisher for the gateway + tonle flow."""

from __future__ import annotations

import os
import sys

import httpx

GATEWAY_BASE_URL = os.getenv("GATEWAY_BASE_URL", "http://127.0.0.1:8010")
GATEWAY_INTERNAL_TOKEN = os.getenv("GATEWAY_INTERNAL_TOKEN", "gateway-internal-secret")
DEMO_TENANT = os.getenv("DEMO_TENANT", "t1")
JOB_ID = os.getenv("JOB_ID", "job-1")


def main() -> None:
    progress = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    status = sys.argv[2] if len(sys.argv) > 2 else "running"
    message = sys.argv[3] if len(sys.argv) > 3 else f"progress {progress}%"

    payload = {
        "status": status,
        "progress": progress,
        "message": message,
    }
    if progress >= 100:
        payload["result"] = {"ok": True}

    with httpx.Client(base_url=GATEWAY_BASE_URL, timeout=30.0) as client:
        response = client.post(
            f"/internal/tenants/{DEMO_TENANT}/jobs/{JOB_ID}/events",
            headers={"Authorization": f"Bearer {GATEWAY_INTERNAL_TOKEN}"},
            json=payload,
        )
        response.raise_for_status()
        print(response.json())


if __name__ == "__main__":
    main()
