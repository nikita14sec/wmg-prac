from ArtifactPipeline.client import ArtifactPipelineClient
from ArtifactPipeline.core import Orders, Asset, Metadata, ProcessedOrder
stages=[
    'order_received',
    'asset_generation_triggered',
    'asset_generation_polling',
    'asset_generation_completed',
    'metadata_extraction_triggered',
    'metadata_extraction_polling',
    'metadata_extraction_completed',
    'order_completed'
]
failure_stages=[
    'asset_generation_failed',
    'metadata_extraction_failed'
]
client = ArtifactPipelineClient(host="http://localhost:5001", headers={"Content-Type": "application/json"})
class ArtifactPipelineOrchestrator:
    def __init__(self, client):
        self.client = client

    def process_order(self, order):
        # take order
        order_data = {}
        try:
            order_data = self.client._request('GET', '/orders/next')
        except Exception as e:
            print(f"Error fetching order: {e}")
            return

        if not order_data or not order_data.get('order_id'):
            return

        order_object = Orders(
            order_id=order_data['order_id'],
            stage=stages[0],
            asset=None,
            metadata=None,
        )
        order_object.validate()
        # trigger asset generation
        try:
            asset_job_id = self.client._request(
                'POST',
                f'/orders/{order_object.order_id}/assets/generate',
            )
        except Exception as e:
            print(f"Error triggering asset generation: {e}")
            order_object.stage = failure_stages[0]
            ProcessedOrder(shippable=False, order=order_object).send_order()
            return
        order_object.stage = stages[1]
        # poll asset generation
        try:
            self.client.poll(
                polling_path=f'/jobs/{asset_job_id["job_id"]}',
                polling_key='status',
                polling_value='success',
            )
        except TimeoutError as e:
            print(f"Asset generation polling timed out: {e}")
            order_object.stage = failure_stages[0]
            ProcessedOrder(shippable=False, order=order_object).send_order()
            return
        order_object.stage = stages[3]
        # get generated assets
        try:
            asset_data = self.client._request(
                'GET',
                f'/orders/{order_object.order_id}/assets',
            )
        except Exception as e:
            print(f"Error fetching generated assets: {e}")
            order_object.stage = failure_stages[0]
            ProcessedOrder(shippable=False, order=order_object).send_order()
            return
        asset_object = Asset(
            order_id=order_object.order_id,
            assets=asset_data['assets'],
            asset_details=asset_data['asset_details'],
        )
        asset_object.validate()
        order_object.asset = asset_data
        # trigger metadata extraction
        try:
            metadata_job_id = self.client._request(
                'POST',
                f'/orders/{order_object.order_id}/metadata/generate',
            )
        except Exception as e:
            print(f"Error triggering metadata extraction: {e}")
            order_object.stage = failure_stages[1]
            ProcessedOrder(shippable=False, order=order_object).send_order()
            return
        order_object.stage = stages[4]
        # poll metadata extraction
        try:
            self.client.poll(
                polling_path=f'/jobs/{metadata_job_id["job_id"]}',
                polling_key='status',
                polling_value='success',
            )
            order_object.stage = stages[5]
        except TimeoutError as e:
            print(f"Metadata extraction polling timed out: {e}")
            order_object.stage = failure_stages[1]
            ProcessedOrder(shippable=False, order=order_object).send_order()
            return
        # get extracted metadata
        order_object.stage = stages[6]
        try:
            metadata_data = self.client._request(
                'GET',
                f'/orders/{order_object.order_id}/metadata',
            )
        except Exception as e:
            print(f"Error fetching extracted metadata: {e}")
            order_object.stage = failure_stages[1]
            ProcessedOrder(shippable=False, order=order_object).send_order()
            return
        order_object.stage = stages[7]
        metadata_object = Metadata(
            order_id=order_object.order_id,
            metadata=metadata_data['metadata'],
        )
        metadata_object.validate()
        order_object.metadata = metadata_data
        # send completed order
        ProcessedOrder(shippable=True, order=order_object).send_order()
        order_object.stage = stages[7]


if __name__ == "__main__":
    import sys
    import requests
    from concurrent.futures import ThreadPoolExecutor
    
    # Reset server
    base_url = "http://localhost:5001"
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 42
    num_workers = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    
    try:
        requests.post(f"{base_url}/reset", json={"seed": seed})
        print(f"✓ Server reset with seed={seed}, spawning {num_workers} workers...")
    except Exception as e:
        print(f"✗ Reset failed: {e}")
        sys.exit(1)
    
    # Create orchestrator
    orch_client = ArtifactPipelineClient(
        host=base_url,
        headers={"Content-Type": "application/json"},
    )
    orchestrator = ArtifactPipelineOrchestrator(orch_client)
    
    # Worker function
    def worker(worker_id):
        count = 0
        while True:
            try:
                result = orchestrator.process_order()
                if result is None:
                    break
                count += 1
            except Exception as e:
                print(f"[W{worker_id}] Error: {e}")
        print(f"[W{worker_id}] Finished: {count} orders")
        return count
    
    # Run parallel
    with ThreadPoolExecutor(max_workers=num_workers) as pool:
        counts = list(pool.map(worker, range(num_workers)))
    
    # Show score
    try:
        score = requests.get(f"{base_url}/score").json()
        print(f"\n📊 Final score: {score['score']}/{score['total_orders']}")
    except:
        pass

        