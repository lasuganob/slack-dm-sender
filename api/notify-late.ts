import type { VercelRequest, VercelResponse } from '@vercel/node';

const BOT = process.env.SLACK_BOT_TOKEN || '';
const API = process.env.API_BEARER_TOKEN || '';

type UserItem = {
  slack_id: string;           // e.g. "U06ABCDEF"
  room_url: string;
  custom_message?: string;
};

async function slack(path: string, payload: unknown) {
  const resp = await fetch(`https://slack.com/api/${path}`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${BOT}`,
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: JSON.stringify(payload),
  });
  const json = await resp.json().catch(() => ({}));
  return { status: resp.status, headers: resp.headers, json };
}

export default async function handler(req: VercelRequest, res: VercelResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ ok: false, error: 'method_not_allowed' });
  }

  if (!BOT) {
    return res.status(500).json({ ok: false, error: 'missing_SLACK_BOT_TOKEN' });
  }

  // Bearer auth for your callers
  const auth = req.headers.authorization || '';
  if (API && auth !== `Bearer ${API}`) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }

  const body = (req.body || {}) as { users?: UserItem[]; default_message?: string };
  const users = body.users;
  if (!Array.isArray(users) || users.length === 0) {
    return res.status(422).json({ ok: false, error: 'users_array_required' });
  }

  const defaultMessage = body.default_message;
  const results: any[] = [];
  const delayMs = 250; // gentle pacing to avoid Slack 429s

  for (const u of users) {
    const slack_id = u?.slack_id;
    const room_url = u?.room_url;
    const text = (u?.custom_message || defaultMessage || "Heads up — you’re 1 minute late for your scheduled class.");

    if (!slack_id || !/^U[A-Z0-9]+$/i.test(slack_id) || typeof room_url !== 'string') {
      results.push({ slack_id, ok: false, error: 'invalid_user_entry' });
      continue;
    }

    // 1) Open (or find) a DM channel
    const open = await slack('conversations.open', { users: slack_id });
    if (!open.json?.ok) {
      results.push({
        slack_id,
        ok: false,
        step: 'open',
        error: open.json?.error || 'open_failed',
        retry_after: open.headers.get('Retry-After') || undefined,
      });
      await new Promise(r => setTimeout(r, delayMs));
      continue;
    }

    const channel = open.json.channel.id as string;

    // 2) Send the DM
    const blocks = [
      { type: 'header', text: { type: 'plain_text', text: "Heads up — you're 1 minute late" } },
      { type: 'section', text: { type: 'mrkdwn', text: `*Room:* <${room_url}|Join now>` } },
      { type: 'actions', elements: [
          { type: 'button', text: { type: 'plain_text', text: 'Join Room' }, url: room_url }
        ]
      }
    ];

    const send = await slack('chat.postMessage', { channel, text, blocks });
    if (!send.json?.ok) {
      results.push({
        slack_id,
        ok: false,
        step: 'send',
        error: send.json?.error || 'send_failed',
        retry_after: send.headers.get('Retry-After') || undefined,
      });
    } else {
      results.push({ slack_id, ok: true, channel, ts: send.json.ts });
    }

    await new Promise(r => setTimeout(r, delayMs));
  }

  return res.status(200).json({ ok: true, results });
}
