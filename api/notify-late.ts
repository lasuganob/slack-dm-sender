import type { VercelRequest, VercelResponse } from '@vercel/node';

/** ===== Env & Tunables ===== */
const BOT = process.env.SLACK_BOT_TOKEN || '';
const API = process.env.API_BEARER_TOKEN || '';

const INTERNAL_BATCH_SIZE = parseInt(process.env.INTERNAL_BATCH_SIZE || '12', 10);   // users per internal wave
const CONCURRENCY        = parseInt(process.env.CONCURRENCY || '4', 10);            // workers per wave
const PACE_MS            = parseInt(process.env.PACE_MS || '300', 10);              // delay per worker step
const MAX_EXECUTION_MS   = parseInt(process.env.MAX_EXECUTION_MS || '9000', 10);    // keep under Hobby timeout
const CHANNEL_TTL_MS     = parseInt(process.env.CHANNEL_TTL_MS || String(15*60*1000), 10);
const IDEMPOTENCY_TTL_MS = parseInt(process.env.IDEMPOTENCY_TTL_MS || String(10*60*1000), 10);
const PER_USER_MAX_RETRIES = parseInt(process.env.PER_USER_MAX_RETRIES || '1', 10); // extra tries on 429/network

/** ===== Types ===== */
type UserItem = {
  slack_id: string;
  room_url: string;
  custom_message?: string;
  user_key?: string;  // optional, used with batch_id for idempotency
};
type Body = {
  users?: UserItem[];
  default_message?: string;
  batch_id?: string;          // e.g., "2025-11-06T09:35"
  resume_users_b64?: string;  // server-provided resume payload (opaque)
};

/** ===== Caches (per instance) ===== */
const channelCache = new Map<string, { channel: string; exp: number }>();
const sentKeys = new Map<string, number>(); // `${batch_id}::${user_key}` -> exp ts
setInterval(() => {
  const now = Date.now();
  for (const [k, v] of channelCache) if (v.exp <= now) channelCache.delete(k);
  for (const [k, exp] of sentKeys) if (exp <= now) sentKeys.delete(k);
}, 60_000).unref?.();

/** ===== Utils ===== */
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

async function slack(path: string, payload: unknown) {
  const resp = await fetch(`https://slack.com/api/${path}`, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${BOT}`,
      'Content-Type': 'application/json; charset=utf-8',
    },
    body: JSON.stringify(payload),
  });
  let json: any = {};
  try { json = await resp.json(); } catch {}
  return { status: resp.status, headers: resp.headers, json };
}

function encodeResume(users: UserItem[]): string {
  return Buffer.from(JSON.stringify(users)).toString('base64');
}
function decodeResume(b64: string): UserItem[] {
  try { return JSON.parse(Buffer.from(b64, 'base64').toString('utf8')); }
  catch { return []; }
}

/** bounded-parallel map with pacing */
async function mapBounded<T, R>(
  items: T[],
  worker: (item: T, idx: number) => Promise<R>,
  concurrency: number,
  paceMs: number,
) {
  const out: R[] = new Array(items.length);
  let i = 0;
  async function run() {
    while (i < items.length) {
      const idx = i++;
      out[idx] = await worker(items[idx], idx);
      if (paceMs > 0) await sleep(paceMs);
    }
  }
  const workers = Array.from({ length: Math.min(concurrency, items.length) }, run);
  await Promise.all(workers);
  return out;
}

/** one user flow with small retry loop */
async function dmOne(
  user: UserItem,
  defaults: { default_message?: string },
  ctx: { batch_id?: string }
) {
  const { default_message } = defaults;
  const { batch_id } = ctx;

  const slack_id = user?.slack_id;
  const room_url = user?.room_url;
  const text = user?.custom_message || default_message || "You are 1 minute late to join the lesson room.";
  const user_key = user?.user_key || slack_id;

  if (!slack_id || !/^U[A-Z0-9]+$/i.test(slack_id) || typeof room_url !== 'string') {
    return { slack_id, ok: false, error: 'invalid_user_entry' };
  }

  // Idempotency
  if (batch_id && user_key) {
    const idemKey = `${batch_id}::${user_key}`;
    const exp = sentKeys.get(idemKey);
    if (exp && exp > Date.now()) return { slack_id, ok: true, skipped: 'idempotent_duplicate' };
  }

  // open DM (use cache)
  const openOrGetChannel = async () => {
    const cached = channelCache.get(slack_id);
    if (cached && cached.exp > Date.now()) return cached.channel;

    const open = await slack('conversations.open', { users: slack_id });
    if (!open.json?.ok) {
      const retryAfter = open.headers.get('Retry-After');
      return { error: open.json?.error || 'open_failed', retry_after: retryAfter ? parseInt(retryAfter, 10) : undefined } as const;
    }
    const channel = open.json.channel.id as string;
    channelCache.set(slack_id, { channel, exp: Date.now() + CHANNEL_TTL_MS });
    return channel;
  };

  // small retry loop on 429/network
  let attempt = 0;
  let channelId: string | undefined;
  while (attempt <= PER_USER_MAX_RETRIES) {
    attempt++;
    const ch = await openOrGetChannel();
    if (typeof ch === 'string') { channelId = ch; break; }
    if (ch.error === 'rate_limited' && ch.retry_after) {
      await sleep(ch.retry_after * 1000);
      continue;
    }
    // non-retriable
    return { slack_id, ok: false, step: 'open', error: ch.error, retry_after: ch.retry_after };
  }
  if (!channelId) {
    return { slack_id, ok: false, step: 'open', error: 'open_failed' };
  }

  // send message (with Blocks + text fallback)
  const blocks = [
    { type: 'header', text: { type: 'plain_text', text: "You are 1 minute late to join the lesson room." } },
    { type: 'section', text: { type: 'mrkdwn', text: `*Room url:* ${room_url}` } },
    { type: 'actions', elements: [{ type: 'button', text: { type: 'plain_text', text: 'Join Room' }, url: room_url }] }
  ];

  attempt = 0;
  while (attempt <= PER_USER_MAX_RETRIES) {
    attempt++;
    const send = await slack('chat.postMessage', { channel: channelId, text, blocks });
    if (send.json?.ok) {
      if (batch_id && user_key) {
        const idemKey = `${batch_id}::${user_key}`;
        sentKeys.set(idemKey, Date.now() + IDEMPOTENCY_TTL_MS);
      }
      return { slack_id, ok: true, channel: channelId, ts: send.json.ts };
    }
    const retryAfter = send.headers.get('Retry-After');
    if (send.json?.error === 'rate_limited' && retryAfter) {
      await sleep(parseInt(retryAfter, 10) * 1000);
      continue;
    }
    // non-retriable
    return { slack_id, ok: false, step: 'send', error: send.json?.error || 'send_failed', retry_after: retryAfter ? parseInt(retryAfter, 10) : undefined };
  }

  return { slack_id, ok: false, step: 'send', error: 'send_failed' };
}

/** ===== HTTP entry ===== */
export default async function handler(req: VercelRequest, res: VercelResponse) {
  const started = Date.now();

  try {
    if (req.method !== 'POST') return res.status(405).json({ ok: false, error: 'method_not_allowed' });
    if (!BOT)            return res.status(500).json({ ok: false, error: 'missing_SLACK_BOT_TOKEN' });

    // bearer
    const auth = req.headers.authorization || '';
    if (API && auth !== `Bearer ${API}`) return res.status(401).json({ ok: false, error: 'unauthorized' });

    const body = (req.body || {}) as Body;

    // Accept either fresh users or a server-provided resume payload
    let incoming: UserItem[] = [];
    if (Array.isArray(body.users)) incoming = body.users;
    else if (body.resume_users_b64) incoming = decodeResume(body.resume_users_b64);

    if (!Array.isArray(incoming) || incoming.length === 0) {
      return res.status(422).json({ ok: false, error: 'users_array_required' });
    }

    const defaults = { default_message: body.default_message };
    const batch_id = body.batch_id;

    const allResults: any[] = [];
    let remaining = incoming.slice(0); // clone

    // process in internal waves until time budget runs out or finished
    while (remaining.length > 0) {
      const elapsed = Date.now() - started;
      const timeLeft = MAX_EXECUTION_MS - elapsed;
      if (timeLeft <= Math.max(1500, PACE_MS * 2)) break; // leave buffer to respond

      const wave = remaining.splice(0, INTERNAL_BATCH_SIZE);
      const waveResults = await mapBounded<UserItem, any>(
        wave,
        (u) => dmOne(u, defaults, { batch_id }),
        CONCURRENCY,
        PACE_MS
      );
      allResults.push(...waveResults);
    }

    // summarize
    const successes = allResults.filter(r => r?.ok === true).length;
    const skipped   = allResults.filter(r => r?.skipped === 'idempotent_duplicate').length;
    const failures  = allResults.filter(r => !r?.ok && !r?.skipped).length;
    const incomplete = remaining.length > 0;

    // If not finished, return a resume payload the caller can POST back (no custom chunking).
    const resume_users_b64 = incomplete ? encodeResume(remaining) : undefined;

    // If incomplete, 202 Accepted is a nice signal; otherwise 200 OK
    const status = incomplete ? 202 : 200;

    return res.status(status).json({
      ok: failures === 0 && !incomplete,
      meta: {
        received: incoming.length,
        processed: allResults.length,
        remaining: remaining.length,
        incomplete,
        time_ms: Date.now() - started,
        limits: {
          INTERNAL_BATCH_SIZE, CONCURRENCY, PACE_MS, MAX_EXECUTION_MS,
          CHANNEL_TTL_MS, IDEMPOTENCY_TTL_MS, PER_USER_MAX_RETRIES
        }
      },
      summary: { successes, skipped, failures },
      results: allResults,
      resume: resume_users_b64 ? { resume_users_b64, hint: "POST this value back to the same endpoint with the same headers/body (batch_id optional), to continue." } : undefined
    });
  } catch (err: any) {
    return res.status(500).json({ ok: false, error: 'unhandled_exception', message: err?.message || String(err) });
  }
}
