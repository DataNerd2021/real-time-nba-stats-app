import requests
from dotenv import load_dotenv
import json

import os
load_dotenv()

url = "https://api.balldontlie.io/v1/players"


params = {
    "per_page": 1, 
    "page": 1     
}

headers = {
    "Authorization": os.getenv('BALLDONTLIE_API_KEY')
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    data = response.json()
    
    # Pretty print the JSON response with indentation
    print(json.dumps(data, indent=4))
else:
    print(f"Failed to fetch data: {response.status_code}")