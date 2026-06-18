"""
MOCK PRACTICE TEST SERVER
=========================
This is NOT the real WMG test server — the real one will be given to you
in CoderPad. This is a stand-in I built so you can practice the *pattern*
of the interview (poll a queue, trigger async jobs, poll for completion,
submit results, handle failures) using a generic API shape.

The real server's exact endpoint names/payloads WILL differ. Read the
real docs carefully on interview day. But the SHAPE of the problem
(queue -> async job -> poll status -> get result -> submit) is a very
common pattern, so practicing against this will build the right muscle
memory: writing a polling loop, handling failure branches, and submitting
final results without breaking state.

HOW TO RUN:
    pip install flask
    python mock_testserver.py
Server runs on http://localhost:5000

ENDPOINTS (mock shape):
    GET  /orders/next
        -> {"order_id": "...", "status": "queued"} or {"order_id": null}
        Each call "takes" the next order off the queue (can only be taken once).

    POST /orders/<order_id>/assets/generate
        -> {"job_id": "..."}
        Kicks off async asset generation for this order.

    GET  /jobs/<job_id>
        -> {"job_id": "...", "status": "pending"|"success"|"failed"}
        Poll this to check job status. Status starts as "pending" and
        randomly resolves after a few polls.

    GET  /orders/<order_id>/assets
        -> {"assets": [...], "asset_details": [...]}
        Only available after asset generation job succeeds.

    POST /orders/<order_id>/metadata/generate
        -> {"job_id": "..."}
        Kicks off async metadata generation. Will error if asset
        generation hasn't succeeded yet (enforces orchestration rule).

    GET  /orders/<order_id>/metadata
        -> {"metadata": {...}}
        Only available after metadata generation job succeeds.

    POST /submit/shippable
        body: {"order_id": "...", "assets": [...], "asset_details": [...], "metadata": {...}}
        -> {"accepted": true/false, "message": "..."}

    POST /submit/failed
        body: {"order_id": "...", "assets": [...]|null, "asset_details": [...]|null,
               "stage": "asset"|"metadata"}
        -> {"accepted": true/false, "message": "..."}

    GET  /score
        -> {"score": N, "total_orders": N, "details": [...]}
        Check your running score / submission correctness at any time.

    POST /reset
        -> resets all state, generates a fresh queue of orders.
        Useful for re-running practice attempts from scratch.

NOTES ON RANDOM FAILURES (tweak these constants below to make practice
harder/easier):
    - ASSET_FAIL_RATE: probability an asset generation job fails
    - METADATA_FAIL_RATE: probability a metadata generation job fails
    - JOB_POLL_ATTEMPTS: how many times you must poll before a job resolves
      (simulates "API may not always be reliable / instant")
"""

import random
import os
import uuid
import time
from flask import Flask, jsonify, request

app = Flask(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIG — tweak to change difficulty
# ─────────────────────────────────────────────────────────────
NUM_ORDERS = 10
ASSET_FAIL_RATE = 0.2        # base prob asset gen fails (overridden per-edge-case below)
METADATA_FAIL_RATE = 0.2     # base prob metadata gen fails
JOB_POLL_MIN_ATTEMPTS = 1     # min polls before a job resolves
JOB_POLL_MAX_ATTEMPTS = 3     # max polls before a job resolves

# Probability (0-1) that /jobs/<id> returns a flaky 500 error on any
# given poll (simulates "APIs may not always be reliable")
FLAKY_500_RATE = 0.15

# Probability that /jobs/<id> returns malformed JSON instead of the
# expected shape (tests your client's error handling)
MALFORMED_RESPONSE_RATE = 0.05


# ─────────────────────────────────────────────────────────────
# EDGE CASE ORDER TYPES
# Each order is assigned a "scenario" that controls how its jobs behave.
# This gives you VARIED, REPRODUCIBLE edge cases to test against,
# instead of pure randomness every time.
# ─────────────────────────────────────────────────────────────

SCENARIOS = [
    # name,                 asset_outcome, metadata_outcome, notes
    ("happy_path",          "success",     "success",  "Everything works normally"),
    ("asset_fails",         "failed",      None,       "Asset gen fails -> FailedOrder, no metadata triggered"),
    ("metadata_fails",      "success",     "failed",   "Asset ok, metadata fails -> FailedOrder w/ asset data"),
    ("slow_asset",          "success",     "success",  "Asset job takes many polls before resolving"),
    ("instant_success",     "success",     "success",  "Resolves on first poll - tests you don't assume min polls"),
    ("flaky_then_success",  "success",     "success",  "Several flaky 500s before job status is retrievable"),
    ("both_fail",           "failed",      None,       "Asset fails, must not trigger metadata at all"),
    ("slow_metadata",       "success",     "success",  "Metadata job takes many polls"),
    ("malformed_then_ok",   "success",     "success",  "Malformed JSON responses mixed in before valid ones"),
    ("happy_path_2",        "success",     "success",  "Another normal one, for volume"),
]


# ─────────────────────────────────────────────────────────────
# STATE (in-memory, reset via /reset)
# ─────────────────────────────────────────────────────────────
state = {}


def fresh_state(seed=None):
    """
    Build a fresh queue of orders.

    If `seed` is given, randomness is reproducible (useful for
    re-running the exact same scenario set while debugging your client).
    Scenarios are assigned round-robin from SCENARIOS so you get a
    *varied* but *known* mix of edge cases every run.
    """
    if seed is not None:
        random.seed(seed)

    orders = {}
    queue = []
    for i in range(NUM_ORDERS):
        order_id = f"order-{i+1}"
        scenario_name, asset_outcome, metadata_outcome, _ = SCENARIOS[i % len(SCENARIOS)]

        # Resolve timing per scenario
        if scenario_name == "slow_asset":
            asset_resolve_at = JOB_POLL_MAX_ATTEMPTS + 2
        elif scenario_name == "instant_success":
            asset_resolve_at = 1
        else:
            asset_resolve_at = random.randint(JOB_POLL_MIN_ATTEMPTS, JOB_POLL_MAX_ATTEMPTS)

        if scenario_name == "slow_metadata":
            metadata_resolve_at = JOB_POLL_MAX_ATTEMPTS + 2
        else:
            metadata_resolve_at = random.randint(JOB_POLL_MIN_ATTEMPTS, JOB_POLL_MAX_ATTEMPTS)

        orders[order_id] = {
            "order_id": order_id,
            "scenario": scenario_name,   # visible via /score for debugging, not via normal endpoints
            "taken": False,
            "asset_job_id": None,
            "asset_status": None,        # None | pending | success | failed
            "asset_poll_count": 0,
            "asset_resolve_at": asset_resolve_at,
            "asset_forced_outcome": asset_outcome,
            "flaky_polls_remaining_asset": 2 if scenario_name == "flaky_then_success" else 0,
            "malformed_polls_remaining_asset": 2 if scenario_name == "malformed_then_ok" else 0,
            "assets": [
                {"asset_id": f"{order_id}-asset-1", "type": "audio", "filename": "track.wav"},
                {"asset_id": f"{order_id}-asset-2", "type": "coverart", "filename": "cover.tiff"},
            ],
            "asset_details": [
                {"asset_id": f"{order_id}-asset-1", "duration_sec": 215, "bitrate": "320kbps"},
                {"asset_id": f"{order_id}-asset-2", "width": 3000, "height": 3000},
            ],
            "metadata_job_id": None,
            "metadata_status": None,
            "metadata_poll_count": 0,
            "metadata_resolve_at": metadata_resolve_at,
            "metadata_forced_outcome": metadata_outcome,
            "metadata": {
                "title": f"Track {i+1}",
                "artist": "Sample Artist",
                "isrc": f"US-MOCK-{i+1:04d}",
            },
            "submitted": False,
            "submission_correct": None,
            "submission_type": None,
        }
        queue.append(order_id)

    return {
        "orders": orders,
        "queue": queue,
        "jobs": {},   # job_id -> {"order_id":..., "kind": "asset"|"metadata"}
        "submissions": [],
    }


state = fresh_state()


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

class FlakyError(Exception):
    """Raised to simulate a transient 500 from the job status endpoint."""
    pass


class MalformedResponse(Exception):
    """Raised to simulate the endpoint returning unexpected JSON shape."""
    pass


def resolve_job_status(order, kind):
    """
    Returns current status for a job, advancing the poll counter.

    Behavior:
      - For the first few polls (per scenario), may raise FlakyError
        (-> caller should return HTTP 500) or MalformedResponse
        (-> caller should return a weirdly-shaped JSON body).
      - Once "resolve_at" polls have happened, resolves to the
        scenario's forced outcome (success/failed). If no forced
        outcome is set, falls back to random using the FAIL_RATE configs.
    """
    prefix = "asset" if kind == "asset" else "metadata"
    poll_count_key = f"{prefix}_poll_count"
    status_key = f"{prefix}_status"
    resolve_at_key = f"{prefix}_resolve_at"
    forced_key = f"{prefix}_forced_outcome"
    flaky_key = f"flaky_polls_remaining_{prefix}"
    malformed_key = f"malformed_polls_remaining_{prefix}"

    # Flaky 500s — consume these before counting as a "real" poll
    if order.get(flaky_key, 0) > 0:
        order[flaky_key] -= 1
        raise FlakyError()

    # Malformed responses — consume these before counting as a "real" poll
    if order.get(malformed_key, 0) > 0:
        order[malformed_key] -= 1
        raise MalformedResponse()

    # Generic random flakiness on top, for non-scripted scenarios
    if random.random() < FLAKY_500_RATE and order["scenario"] not in (
        "flaky_then_success", "malformed_then_ok"
    ):
        raise FlakyError()
    if random.random() < MALFORMED_RESPONSE_RATE and order["scenario"] not in (
        "flaky_then_success", "malformed_then_ok"
    ):
        raise MalformedResponse()

    order[poll_count_key] += 1
    if order[status_key] == "pending":
        if order[poll_count_key] >= order[resolve_at_key]:
            forced = order.get(forced_key)
            if forced is not None:
                order[status_key] = forced
            else:
                fail_rate = ASSET_FAIL_RATE if kind == "asset" else METADATA_FAIL_RATE
                order[status_key] = "failed" if random.random() < fail_rate else "success"

    return order[status_key]


# ─────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────

@app.route("/orders/next", methods=["GET"])
def take_next_order():
    if not state["queue"]:
        return jsonify({"order_id": None})
    order_id = state["queue"].pop(0)
    state["orders"][order_id]["taken"] = True
    return jsonify({"order_id": order_id, "status": "taken"})


@app.route("/orders/<order_id>/assets/generate", methods=["POST"])
def generate_assets(order_id):
    order = state["orders"].get(order_id)
    if not order:
        return jsonify({"error": "order not found"}), 404
    if not order["taken"]:
        return jsonify({"error": "order must be taken from queue first"}), 400
    if order["asset_status"] is not None:
        return jsonify({"error": "asset generation already triggered"}), 400

    job_id = str(uuid.uuid4())
    order["asset_job_id"] = job_id
    order["asset_status"] = "pending"
    state["jobs"][job_id] = {"order_id": order_id, "kind": "asset"}
    return jsonify({"job_id": job_id})


@app.route("/orders/<order_id>/metadata/generate", methods=["POST"])
def generate_metadata(order_id):
    order = state["orders"].get(order_id)
    if not order:
        return jsonify({"error": "order not found"}), 404
    if order["asset_status"] != "success":
        return jsonify({
            "error": "cannot trigger metadata generation before asset generation succeeds"
        }), 400
    if order["metadata_status"] is not None:
        return jsonify({"error": "metadata generation already triggered"}), 400

    job_id = str(uuid.uuid4())
    order["metadata_job_id"] = job_id
    order["metadata_status"] = "pending"
    state["jobs"][job_id] = {"order_id": order_id, "kind": "metadata"}
    return jsonify({"job_id": job_id})


@app.route("/jobs/<job_id>", methods=["GET"])
def get_job_status(job_id):
    job = state["jobs"].get(job_id)
    if not job:
        return jsonify({"error": "job not found"}), 404

    order = state["orders"][job["order_id"]]
    try:
        status = resolve_job_status(order, job["kind"])
    except FlakyError:
        # Simulate a transient server error - client should retry
        return jsonify({"error": "internal server error, please retry"}), 500
    except MalformedResponse:
        # Simulate an unexpected/malformed response shape (wrong keys,
        # missing "status" field) - client must handle gracefully
        return jsonify({"jobId": job_id, "stat": "unknown", "weird": True})

    return jsonify({"job_id": job_id, "status": status})


@app.route("/orders/<order_id>/assets", methods=["GET"])
def get_assets(order_id):
    order = state["orders"].get(order_id)
    if not order:
        return jsonify({"error": "order not found"}), 404
    if order["asset_status"] != "success":
        return jsonify({"error": "assets not ready"}), 400
    return jsonify({
        "assets": order["assets"],
        "asset_details": order["asset_details"],
    })


@app.route("/orders/<order_id>/metadata", methods=["GET"])
def get_metadata(order_id):
    order = state["orders"].get(order_id)
    if not order:
        return jsonify({"error": "order not found"}), 404
    if order["metadata_status"] != "success":
        return jsonify({"error": "metadata not ready"}), 400
    return jsonify({"metadata": order["metadata"]})


@app.route("/submit/shippable", methods=["POST"])
def submit_shippable():
    body = request.get_json(force=True)
    order_id = body.get("order_id")
    order = state["orders"].get(order_id)
    if not order:
        return jsonify({"accepted": False, "message": "order not found"}), 404
    if order["submitted"]:
        return jsonify({"accepted": False, "message": "order already submitted"}), 400

    correct = (
        order["asset_status"] == "success"
        and order["metadata_status"] == "success"
        and "assets" in body and "asset_details" in body and "metadata" in body
    )

    order["submitted"] = True
    order["submission_type"] = "shippable"
    order["submission_correct"] = correct
    state["submissions"].append(order_id)

    return jsonify({"accepted": correct,
                     "message": "ok" if correct else "incorrect submission for order state"})


@app.route("/submit/failed", methods=["POST"])
def submit_failed():
    body = request.get_json(force=True)
    order_id = body.get("order_id")
    stage = body.get("stage")
    order = state["orders"].get(order_id)
    if not order:
        return jsonify({"accepted": False, "message": "order not found"}), 404
    if order["submitted"]:
        return jsonify({"accepted": False, "message": "order already submitted"}), 400

    correct = False
    if stage == "asset" and order["asset_status"] == "failed":
        # FailedOrder at asset stage should have NO asset/metadata data
        correct = "assets" not in body and "metadata" not in body
    elif stage == "metadata" and order["metadata_status"] == "failed":
        # FailedOrder at metadata stage should HAVE asset data but NO metadata
        correct = "assets" in body and "asset_details" in body and "metadata" not in body

    order["submitted"] = True
    order["submission_type"] = "failed"
    order["submission_correct"] = correct
    state["submissions"].append(order_id)

    return jsonify({"accepted": correct,
                     "message": "ok" if correct else "incorrect submission for order state"})


@app.route("/score", methods=["GET"])
def score():
    total = len(state["orders"])
    correct = sum(1 for o in state["orders"].values() if o["submission_correct"])
    submitted = sum(1 for o in state["orders"].values() if o["submitted"])
    details = [
        {
            "order_id": oid,
            "scenario": o["scenario"],
            "taken": o["taken"],
            "asset_status": o["asset_status"],
            "metadata_status": o["metadata_status"],
            "submitted": o["submitted"],
            "submission_type": o["submission_type"],
            "correct": o["submission_correct"],
        }
        for oid, o in state["orders"].items()
    ]
    return jsonify({
        "score": correct,
        "total_orders": total,
        "submitted": submitted,
        "details": details,
    })


@app.route("/reset", methods=["POST"])
def reset():
    global state
    body = request.get_json(silent=True) or {}
    seed = body.get("seed")
    state = fresh_state(seed=seed)
    return jsonify({"message": "state reset", "num_orders": NUM_ORDERS, "seed": seed})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5001"))
    print(f"Mock test server running. {NUM_ORDERS} orders queued.")
    print("Endpoints: /orders/next, /orders/<id>/assets/generate, /jobs/<id>,")
    print("           /orders/<id>/assets, /orders/<id>/metadata/generate,")
    print("           /orders/<id>/metadata, /submit/shippable, /submit/failed,")
    print("           /score, /reset")
    app.run(host="0.0.0.0", port=port, debug=False)
