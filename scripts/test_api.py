"""Quick test of the async upload API."""
import json
import time

import requests

BASE = "http://localhost:8000"

print("1. Uploading sample_dirty.csv...")
with open("tests/fixtures/sample_dirty.csv", "rb") as f:
    r = requests.post(f"{BASE}/api/clean/upload", files={"file": f})
data = r.json()
print(json.dumps(data, indent=2))

job_id = data["job_id"]

print("\n2. Polling job status...")
for _ in range(60):
    status = requests.get(f"{BASE}/api/jobs/{job_id}").json()
    print(f"   Status: {status['status']} | {status.get('progress', '')}")
    if status["status"] in ("completed", "failed"):
        break
    time.sleep(1)

if status["status"] == "completed":
    result = status["result"]
    print("\n3. RESULTS:")
    print(f"   Rows:        {result['total_rows']}")
    print(f"   Issues:      {result['issues_detected']}")
    print(f"   Fixes:       {result['fixes_applied']}")
    print(f"   Confidence:  {result['overall_confidence']:.1%}")
    print(f"   Validation:  {result['validation_passed']}")
    print(f"   Duration:    {result['duration_seconds']:.1f}s")
    print(f"   Breakdown:   {result['fix_breakdown']}")

    print("\n4. Testing download...")
    dl = requests.get(f"{BASE}/api/download/{job_id}")
    print(f"   Download status: {dl.status_code}, size: {len(dl.content)} bytes")
else:
    print(f"\nFAILED: {status.get('error', 'unknown')}")
