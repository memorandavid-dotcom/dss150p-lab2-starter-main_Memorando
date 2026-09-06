import requests
import json

url = "http://127.0.0.1:8000/api/events?page=1&per_page=10"

print("--- TASK 1.5: REST API PROFILING ---")

try:
    # Retrieve Page 1
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    data = response.json()
    
    print("\nPagination Fields Found:")
    for key in ['page', 'per_page', 'total', 'has_more', 'next_page']:
        print(f"  - {key}: {data.get(key)}")
        
    print(f"\nTotal items retrieved on page 1: {len(data['items'])}")
    if data['items']:
        print("Sample Item Structure:")
        print(json.dumps(data['items'][0], indent=2))

except requests.exceptions.ConnectionError:
    print("ERROR: Could not connect to the API server. Make sure it is running in another terminal!")

