import os
import json
import re
import time
import urllib.request
import urllib.error

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]            # xoxb-…
API_BEARER_TOKEN = os.environ.get("API_BEARER_TOKEN", "")   # shared secret


def _slack(path: str, payload: dict):
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=f"https://slack.com/api/{path}",
        data=data,
        headers={
            "Authorization": f"Bearer {SLACK_BOT_TOKEN}",
            "Content-Type": "application/json; charset=utf-8",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode()), dict(resp.headers), resp.status
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        try:
            j = json.loads(body)
        except Exception:
            j = {"ok": False, "error": "http_error", "body": body}
        return j, dict(getattr(e, "headers", {}) or {}), e.code
    except Exception as e:
        return {"ok": False, "error": f"request_failed:{type(e).__name__}"}, {}, 0


def handler(request):
    if request.method != "POST":
        return (405, {"content-type": "application/json"}, json.dumps({"ok": False, "error": "method_not_allowed"}))

    # Bearer auth
    if API_BEARER_TOKEN and (request.headers.get("authorization") or "") != f"Bearer {API_BEARER_TOKEN}":
        return (401, {"content-type": "application/json"}, json.dumps({"ok": False, "error": "unauthorized"}))

    try:
        body = request.get_json()
    except Exception:
        return (400, {"content-type": "application/json"}, json.dumps({"ok": False, "error": "bad_json"}))

    users = body.get("users")
    if not isinstance(users, list) or not users:
        return (422, {"content-type": "application/json"}, json.dumps({"ok": False, "error": "users_array_required"}))

    default_message = body.get("default_message")
    results, delay = [], 0.25  # gentle pacing

    for u in users:
        slack_id = (u or {}).get("slack_id")
        room_url = (u or {}).get("room_url")
        custom_message = (u or {}).get("custom_message") or default_message
        if not slack_id or not re.match(r"^U[A-Z0-9]+$", slack_id, re.I) or not isinstance(room_url, str):
            results.append({"slack_id": slack_id, "ok": False,
                           "error": "invalid_user_entry"})
            continue

        # 1) open DM
        open_json, open_hdrs, _ = _slack(
            "conversations.open", {"users": slack_id})
        if not open_json.get("ok"):
            results.append({"slack_id": slack_id, "ok": False, "step": "open",
                            "error": open_json.get("error", "open_failed"),
                            "retry_after": open_hdrs.get("Retry-After")})
            time.sleep(delay)
            continue

        channel = open_json["channel"]["id"]

        # 2) send message
        text = custom_message or "Heads up — you’re 1 minute late for your scheduled class."
        blocks = [
            {"type": "header", "text": {"type": "plain_text",
                                        "text": "Heads up — you're 1 minute late"}},
            {"type": "section", "text": {"type": "mrkdwn",
                                         "text": f"*Room:* <{room_url}|Join now>"}},
            {"type": "actions", "elements": [{"type": "button", "text": {
                "type": "plain_text", "text": "Join Room"}, "url": room_url}]},
        ]
        send_json, send_hdrs, _ = _slack(
            "chat.postMessage", {"channel": channel, "text": text, "blocks": blocks})
        if not send_json.get("ok"):
            results.append({"slack_id": slack_id, "ok": False, "step": "send",
                            "error": send_json.get("error", "send_failed"),
                            "retry_after": send_hdrs.get("Retry-After")})
        else:
            results.append({"slack_id": slack_id, "ok": True,
                           "channel": channel, "ts": send_json["ts"]})

        time.sleep(delay)

    return (200, {"content-type": "application/json"}, json.dumps({"ok": True, "results": results}))
