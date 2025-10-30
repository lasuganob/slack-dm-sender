import type { VercelRequest, VercelResponse } from '@vercel/node';

export default async function handler(_req: VercelRequest, res: VercelResponse) {
  return res.status(200).json({ ok: true, pong: true, runtime: 'node', time: new Date().toISOString() });
}
