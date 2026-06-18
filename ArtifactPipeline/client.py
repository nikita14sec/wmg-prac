import time
import requests


class ArtifactPipelineClient:
    def __init__(self, host: str, headers: dict = None, timeout: float = 5.0):
        self.host = host.rstrip("/")
        self.headers = headers or {"Content-Type": "application/json"}
        self.timeout = timeout

    def _request(self, method: str, path: str, body=None, retry: int = 3) -> dict:
        """
        Single HTTP method handler with retry.
        - Retries on 5xx and connection errors (exponential backoff)
        - Does NOT retry on 4xx (logic errors)
        """
        url = self.host + path
        for i in range(retry):
            try:
                resp = requests.request(
                    method.upper(),
                    url,
                    json=body,                  # json= not data=
                    headers=self.headers,
                    timeout=self.timeout,
                )
                # 5xx — transient, retry
                if resp.status_code >= 500:
                    raise requests.HTTPError(response=resp)
                # 4xx — logic error, raise immediately
                resp.raise_for_status()
                return resp.json()

            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code < 500:
                    raise  # 4xx — don't retry
                if i == retry - 1:
                    raise
                time.sleep(2 ** i)  # exponential backoff: 1s, 2s, 4s

            except requests.RequestException:
                if i == retry - 1:
                    raise
                time.sleep(2 ** i)

    def poll(
        self,
        path: str,
        status_key: str = "status",
        success_value: str = "success",
        failure_value: str = "failed",
        interval: float = 2.0,
        timeout: float = 120.0,
    ) -> dict:
        """
        Polls path until status resolves to success or failure.
        Handles:
        - Malformed responses (missing status key) → keep polling
        - 500s → retried by _request
        - Timeout → raises TimeoutError
        Returns the full response dict so caller can check status.
        """
        start = time.time()
        while time.time() - start < timeout:
            try:
                resp = self._request("GET", path)

                # Malformed response — status key missing
                status = resp.get(status_key)
                if status is None:
                    time.sleep(interval)
                    continue

                # Resolved — success or failure
                if status in (success_value, failure_value):
                    return resp

            except Exception:
                pass  # 5xx already retried in _request

            time.sleep(interval)

        raise TimeoutError(f"Poll timed out after {timeout}s: {path}")