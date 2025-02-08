import requests
from datetime import datetime

url = "https://query1.finance.yahoo.com/v8/finance/chart/AMZN"
params = {
    "formatted": "true",
    "interval": "1d",
    "includeAdjustedClose": "false",
    "period1": "1201824000",
    "period2": "1735603200",
    "symbol": "AMZN",
}
headers = {
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

response = requests.get(url, headers=headers, params=params)

if response.status_code == 200:
    response_json = response.json()
    timestamps = response_json["chart"]["result"][0]["timestamp"]
    formatted_timestamps = list(map(lambda x: datetime.utcfromtimestamp(x).strftime('%Y-%m-%d'), timestamps))
    high = response_json["chart"]["result"][0]["indicators"]["quote"][0]["high"]
    volume = response_json["chart"]["result"][0]["indicators"]["quote"][0]["volume"]
    open = response_json["chart"]["result"][0]["indicators"]["quote"][0]["open"]
    close = response_json["chart"]["result"][0]["indicators"]["quote"][0]["close"]
    low = response_json["chart"]["result"][0]["indicators"]["quote"][0]["low"]
    print(response.json())  # Print the JSON response
else:
    print(f"Error: {response.status_code}")