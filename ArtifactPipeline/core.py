class Orders:
    def __init__(self, order_id: str, stage: str, asset=None, metadata=None):
        self.order_id = order_id
        self.stage = stage
        self.asset = asset        # fix: not mutable default dict
        self.metadata = metadata

    def to_dict(self):
        return {
            "order_id": self.order_id,
            "stage": self.stage,
            "asset": self.asset,
            "metadata": self.metadata,
        }

    def validate(self):
        if not self.order_id or not isinstance(self.order_id, str):
            raise ValueError("order_id is required and must be a string")
        if not self.stage:
            raise ValueError("stage is required")
        return True


class Asset:
    def __init__(self, order_id: str, assets: list, asset_details: list):
        self.order_id = order_id
        self.assets = assets
        self.asset_details = asset_details

    def to_dict(self):
        return {
            "order_id": self.order_id,
            "assets": self.assets,
            "asset_details": self.asset_details,
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
    def __init__(self, order_id: str, metadata: dict):
        self.order_id = order_id
        self.metadata = metadata

    def to_dict(self):
        return {
            "order_id": self.order_id,
            "metadata": self.metadata,
        }

    def validate(self):
        if not self.order_id:
            raise ValueError("order_id is required")
        if not self.metadata:
            raise ValueError("metadata is required")
        return True
