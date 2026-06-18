from ArtifactPipeline.client import ArtifactPipelineClient

class Orders:
    def __init__(self,order_id,stage,asset={},metadata={}):
        self.order_id=order_id
        self.stage=stage
        self.asset=asset
        self.metadata=metadata
    def to_dict(self):
        return {
            'order_id': self.order_id,
            'stage': self.stage,
            'asset': self.asset,
            'metadata': self.metadata
        }
    def validate(self):
        if not self.order_id or not isinstance(self.order_id,str):
            raise ValueError("order_id is required and must be a string")
        if not self.stage:
            raise ValueError("stage is required")
        return True

class Asset:
    def __init__(self,order_id,assets,asset_details):
        self.order_id=order_id
        self.assets=assets
        self.asset_details=asset_details
    def to_dict(self):
        return {
            'order_id': self.order_id,
            'assets': self.assets,
            'asset_details': self.asset_details
        }
    def validate(self):
        if not self.order_id:
            raise ValueError("order_id is required")
        if not self.assets:
            raise ValueError("assets is required")
        if not self.asset_details:
            raise ValueError("asset_details is required")
        return True
        
class Metadata:
    def __init__(self,order_id,metadata):
        self.order_id=order_id
        self.metadata=metadata
    def to_dict(self):
        return {
            'order_id': self.order_id,
            'metadata': self.metadata
        }
    def validate(self):
        if not self.order_id:
            raise ValueError("order_id is required")
        if not self.metadata:
            raise ValueError("metadata is required")
        return True
    
class ProcessedOrder:
    def __init__(self, shippable, order)
        self.shippable=shippable
        self.order=order
    def to_dict(self):
        return {
            'shippable': self.shippable,
            'order': self.order.to_dict()
        }
    def send_order():
        client=ArtifactPipelineClient(host='http://localhost:5001/submit/shippable',headers={'Content-Type':'application/json'})
        if not self.shippable:
            client._request('POST', '/submit/shippable', data=self.to_dict())
        else:
            client._request('POST', '/submit/non-shippable', data=self.to_dict())