from dotenv import load_dotenv
import httpx
import os

load_dotenv()

class RavelryClient:
    def __init__(self):
        self.base_url = "https://api.ravelry.com"
        self.auth = (os.getenv("RAVELRY_USER"), os.getenv("RAVELRY_PASSWORD"))
        self.client = httpx.Client(base_url=self.base_url, auth=self.auth)

    def fetch_endpoint(self, endpoint: str, params: dict = None) -> dict:
        try:
            response = self.client.get(endpoint, params=params)
            response.raise_for_status()

            try:
                data = response.json()
                return data
            except JSONDecodeError:
                print("Not JSON")
                # handle if not JSON

        except httpx.HTTPStatusError as e:
            # handle other errors
            print("ERROR")
            print(e)
        
        except:
            print("UNKNOWN ERROR")
