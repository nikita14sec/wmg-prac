import sys
import requests
from concurrent.futures import ThreadPoolExecutor

from ArtifactPipeline.client import ArtifactPipelineClient
from ArtifactPipeline.core import Orders, Asset, Metadata

# Stage constants — use strings directly, not fragile index access
STAGE_ORDER_RECEIVED = "order_received"
STAGE_ASSET_TRIGGERED = "asset_generation_triggered"
STAGE_ASSET_POLLING = "asset_generation_polling"
STAGE_ASSET_COMPLETED = "asset_generation_completed"
STAGE_METADATA_TRIGGERED = "metadata_extraction_triggered"
STAGE_METADATA_POLLING = "metadata_extraction_polling"
STAGE_METADATA_COMPLETED = "metadata_extraction_completed"
STAGE_ORDER_COMPLETED = "order_completed"

STAGE_ASSET_FAILED = "asset_generation_failed"
STAGE_METADATA_FAILED = "metadata_extraction_failed"


class ArtifactPipelineOrchestrator:
    def __init__(self, client: ArtifactPipelineClient):
        self.client = client

    def _submit_failed(
        self,
        order_id: str,
        stage: str,
        assets: list = None,
        asset_details: list = None,
    ):
        """
        Submit a FailedOrder.
        stage=asset  → no asset data
        stage=metadata → include asset data
        """
        body = {"order_id": order_id, "stage": stage}
        if assets:
            body["assets"] = assets
            body["asset_details"] = asset_details
        self.client._request("POST", "/submit/failed", body=body)
        print(f"[{order_id}] ❌ FailedOrder — stage={stage}")

    def _submit_shippable(
        self,
        order_id: str,
        assets: list,
        asset_details: list,
        metadata: dict,
    ):
        """Submit a ShippableOrder with all data."""
        self.client._request("POST", "/submit/shippable", body={
            "order_id": order_id,
            "assets": assets,
            "asset_details": asset_details,
            "metadata": metadata,
        })
        print(f"[{order_id}] ✅ ShippableOrder")

    def process_order(self):
        """
        Full state machine for one order.
        Returns None when queue is empty, True when order processed.

        Rules enforced:
        1. Take before anything else
        2. Assets before metadata
        3. No metadata if assets failed
        4. Asset fail → FailedOrder (no data)
        5. Metadata fail → FailedOrder (with asset data)
        6. Both succeed → ShippableOrder
        """
        # ── Step 1: Take order ────────────────────────────
        try:
            order_data = self.client._request("GET", "/orders/next")
        except Exception as e:
            print(f"Error taking order: {e}")
            return None

        order_id = order_data.get("order_id")
        if not order_id:
            return None  # queue empty

        print(f"[{order_id}] Taken")

        # ── Step 2: Trigger asset generation ─────────────
        try:
            resp = self.client._request(
                "POST", f"/orders/{order_id}/assets/generate"
            )
            asset_job_id = resp["job_id"]
        except Exception as e:
            print(f"[{order_id}] Asset generation trigger failed: {e}")
            self._submit_failed(order_id, stage="asset")
            return True

        # ── Step 3: Poll asset job ─────────────────────
        try:
            poll_result = self.client.poll(f"/jobs/{asset_job_id}")
        except TimeoutError:
            print(f"[{order_id}] Asset poll timed out")
            self._submit_failed(order_id, stage="asset")
            return True

        if poll_result.get("status") == "failed":
            print(f"[{order_id}] Asset generation failed")
            self._submit_failed(order_id, stage="asset")
            return True

        # ── Step 4: Get assets ────────────────────────────
        try:
            asset_data = self.client._request(
                "GET", f"/orders/{order_id}/assets"
            )
            assets = asset_data["assets"]
            asset_details = asset_data["asset_details"]

            # validate
            Asset(order_id=order_id, assets=assets, asset_details=asset_details).validate()
        except Exception as e:
            print(f"[{order_id}] Error fetching assets: {e}")
            self._submit_failed(order_id, stage="asset")
            return True

        # ── Step 5: Trigger metadata generation ──────────
        # Rule 2 — only here because assets succeeded
        try:
            resp = self.client._request(
                "POST", f"/orders/{order_id}/metadata/generate"
            )
            meta_job_id = resp["job_id"]
        except Exception as e:
            print(f"[{order_id}] Metadata generation trigger failed: {e}")
            self._submit_failed(order_id, stage="metadata", assets=assets, asset_details=asset_details)
            return True

        # ── Step 6: Poll metadata job ─────────────────────
        try:
            poll_result = self.client.poll(f"/jobs/{meta_job_id}")
        except TimeoutError:
            print(f"[{order_id}] Metadata poll timed out")
            self._submit_failed(order_id, stage="metadata", assets=assets, asset_details=asset_details)
            return True

        if poll_result.get("status") == "failed":
            print(f"[{order_id}] Metadata generation failed")
            self._submit_failed(order_id, stage="metadata", assets=assets, asset_details=asset_details)
            return True

        # ── Step 7: Get metadata ──────────────────────────
        try:
            meta_data = self.client._request(
                "GET", f"/orders/{order_id}/metadata"
            )
            metadata = meta_data["metadata"]
            Metadata(order_id=order_id, metadata=metadata).validate()
        except Exception as e:
            print(f"[{order_id}] Error fetching metadata: {e}")
            self._submit_failed(order_id, stage="metadata", assets=assets, asset_details=asset_details)
            return True

        # ── Step 8: Submit shippable ──────────────────────
        # Rule 6 — both succeeded
        try:
            self._submit_shippable(order_id, assets, asset_details, metadata)
        except Exception as e:
            print(f"[{order_id}] Submit shippable failed: {e}")

        return True


# ── Entry Point ───────────────────────────────────────

def main():
    base_url = "http://localhost:5001"
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 42
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 5

    # Reset server
    try:
        requests.post(f"{base_url}/reset", json={"seed": seed})
        print(f"✓ Server reset — seed={seed}, workers={num_workers}")
    except Exception as e:
        print(f"✗ Reset failed: {e}")
        sys.exit(1)

    # Build client + orchestrator
    client = ArtifactPipelineClient(
        host=base_url,
        headers={"Content-Type": "application/json"},
    )
    orchestrator = ArtifactPipelineOrchestrator(client)

    # Worker — drains queue until empty
    def worker(worker_id: int) -> int:
        count = 0
        while True:
            try:
                result = orchestrator.process_order()
                if result is None:
                    break  # queue empty
                count += 1
            except Exception as e:
                print(f"[W{worker_id}] Unexpected error: {e}")
                break
        print(f"[W{worker_id}] Done — {count} orders processed")
        return count

    # Run parallel workers
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        counts = list(pool.map(worker, range(num_workers)))

    total = sum(counts)
    print(f"\nTotal orders processed: {total}")

    # Final score
    try:
        score = requests.get(f"{base_url}/score").json()
        print(f"📊 Score: {score['score']}/{score['total_orders']}")
        for d in score["details"]:
            status = "✅" if d["correct"] else "❌" if d["submitted"] else "⬜"
            print(f"  {status} {d['order_id']} | {d['scenario']} | {d.get('submission_type', 'not submitted')}")
    except Exception as e:
        print(f"Score fetch failed: {e}")


if __name__ == "__main__":
    main()