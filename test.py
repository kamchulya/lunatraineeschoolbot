import urllib.request, json

data = json.dumps({
    "name": "Champion Instagram Bot",
    "message_hook_url": "https://championbotinsta-production.up.railway.app/envy_hook",
    "send_local": False,
    "is_active": True,
    "integrations": [{"id": 46, "channel_key": "b21008c1-4fe7-447b-9333-0b47fbc44578"}]
}).encode()

req = urllib.request.Request(
    "https://champion.envycrm.com/openapi/v1/messenger/hook/create",
    data=data,
    method="POST",
    headers={
        "X-Api-Key": "8e9941a4ff86b27e9d3238c8c9732076c7bc74e5",
        "Content-Type": "application/json"
    }
)
try:
    result = json.loads(urllib.request.urlopen(req).read())
    print(json.dumps(result, indent=2, ensure_ascii=False))
except Exception as e:
    print(f"Error: {e}")
