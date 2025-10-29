def handler(request):
    return (200, {"content-type": "application/json"}, '{"ok": true, "pong": true}')
