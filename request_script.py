from dotenv import load_dotenv
import os
import requests

load_dotenv()

api_key = os.getenv("FLASK_API_KEY")
base_url = os.getenv("FLASK_BASE_URL")

response = requests.post(f"{base_url}/causas", headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"})

print(response.json())