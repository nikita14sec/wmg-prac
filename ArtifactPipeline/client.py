import time
import requests

class ArtifactPipelineClient:
    def __init__(self, host, headers, timeout=5.0):
        self.host=host
        self.headers=headers 
        self.timeout=timeout
    def _request(self, method, path, retry=3, **kwargs):
        url=self.host+path
        for i in range(retry):
            try:
                if method.upper()=='GET':
                    response=requests.get(url,headers=self.headers,params=kwargs.get('params',None),data=kwargs.get('data',None),\
                                          timeout=self.timeout)
                    response.raise_for_status()
                    return response.json()
                elif method.upper()=='POST':
                    response=requests.post(url,headers=self.headers,params=kwargs.get('params',None),data=kwargs.get('data',None),\
                                        timeout=self.timeout)
                    response.raise_for_status()
                    return response.json()
                elif method.upper()=='PUT':
                    response=requests.put(url,headers=self.headers,params=kwargs.get('params',None),data=kwargs.get('data',None),\
                                        timeout=self.timeout)
                    response.raise_for_status()
                    return response.json()
                elif method.upper()=='DELETE':
                    response=requests.delete(url,headers=self.headers,params=kwargs.get('params',None),data=kwargs.get('data',None),\
                                        timeout=self.timeout)
                    response.raise_for_status()
                    return response.json()
                
            except requests.exceptions.RequestException as e:
                if i < retry - 1:
                    time.sleep(1)  # Wait for 1 second before retrying
                    continue
                else:
                    raise e
                
        def poll(self,polling_path, polling_key, polling_value, interval=5, timeout=300):
            start_time = time.time()
            while time.time() - start_time < timeout:
                response = self._request('GET', polling_path)
                if response.get(polling_key) == polling_value:
                    return response
                time.sleep(interval)
            raise TimeoutError(f"Polling timed out after {timeout} seconds.")
        