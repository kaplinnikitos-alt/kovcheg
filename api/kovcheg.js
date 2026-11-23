// Сервер «Ковчега»: хранит комнату, игроков и «почтовые ящики» в Upstash Redis (та же база, что у Бункера).
const crypto = require('crypto');
const U = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const T = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const TTL = '43200';
class E extends Error { constructor(m, s) { super(m); this.user = 1; this.status = s || 400; } }
async function pipe(c) {
  const r = await fetch(U + '/pipeline', { method: 'POST', headers: { Authorization: 'Bearer ' + T, 'Content-Type': 'application/json' }, body: JSON.stringify(c) });
  const j = await r.json();
  if (!Array.isArray(j)) throw new Error('redis');
  return j.map(x => { if (x.error) throw new Error(x.error); return x.result; });
}
const K = (c, s) => `kv:${c}:${s}`, rid = () => crypto.randomBytes(8).toString('hex');
const S = (x, n) => String(x || '').trim().slice(0, n);
const norm = c => String(c || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
const vals = a => (a || []).filter((_, i) => i % 2).map(x => JSON.parse(x));
const pub = p => { const { tok, ...x } = p; return x; };
const AR = ['pragmatist', 'observer', 'adventurer'];
const mkP = (n, h) => ({ player_id: rid(), tok: rid(), player_name: n, gender: 'male', archetype: 'pragmatist', is_host: h, is_ready: false, t: Date.now() });
const saveP = (c, p) => pipe([['HSET', K(c, 'p'), p.player_id, JSON.stringify(p)], ['EXPIRE', K(c, 'p'), TTL]]);
const saveR = (c, r) => pipe([['SET', K(c, 'r'), JSON.stringify(r), 'EX', TTL]]);
async function load(code) {
  const c = norm(code); if (c.length !== 5) throw new E('Неверный код комнаты', 404);
  const [r] = await pipe([['GET', K(c, 'r')]]); if (!r) throw new E('Комната не найдена', 404);
  return { c, room: JSON.parse(r) };
}
async function auth(b) {
  const { c, room } = await load(b.code);
  const [p] = await pipe([['HGET', K(c, 'p'), String(b.pid || '-')]]);
  if (!p) throw new E('Игрок не найден', 403);
  const pl = JSON.parse(p); if (pl.tok !== b.tok) throw new E('Нет доступа', 403);
  return { c, room, pl };
}
const A = {
  async create(b) {
    const host = S(b.host, 24), title = S(b.title, 32), max = +b.max, days = +b.days;
    if (!host || !title) throw new E('Укажите имя капитана и название Ковчега.');
    if (!(max >= 6 && max <= 30)) throw new E('Количество игроков должно быть от 6 до 30.');
    if (!(days >= 4 && days <= 10)) throw new E('Количество дней должно быть от 4 до 10.');
    const L = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    for (let i = 0; i < 8; i++) {
      let c = ''; for (let j = 0; j < 5; j++) c += L[crypto.randomInt(32)];
      const room = { code: c, host_name: host, ark_title: title, max_players: max, target_days: days, status: 'waiting', t: Date.now() };
      const [ok] = await pipe([['SET', K(c, 'r'), JSON.stringify(room), 'EX', TTL, 'NX']]);
      if (ok === 'OK') { const pl = mkP(host, true); await saveP(c, pl); return { room, me: pub(pl), tok: pl.tok }; }
    }
    throw new Error('code');
  },
  async join(b) {
    const { c, room } = await load(b.code), n = S(b.name, 24);
    if (!n) throw new E('Введите своё имя.');
    if (room.status !== 'waiting') throw new E('Комната не найдена или игра уже началась.');
    const [len] = await pipe([['HLEN', K(c, 'p')]]);
    if (len >= room.max_players) throw new E('В комнате уже нет свободных мест.');
    const pl = mkP(n, false); await saveP(c, pl);
    return { room, me: pub(pl), tok: pl.tok };
  },
  async upd(b) {
    const { c, room, pl } = await auth(b);
    if (room.status !== 'waiting') throw new E('Игра уже началась');
    const n = S(b.name, 24); if (n) pl.player_name = n;
    if (b.gender === 'male' || b.gender === 'female') pl.gender = b.gender;
    if (AR.includes(b.archetype)) pl.archetype = b.archetype;
    if (typeof b.ready === 'boolean') pl.is_ready = b.ready;
    await saveP(c, pl);
    if (pl.is_host && n) { room.host_name = n; await saveR(c, room); }
    return { ok: 1 };
  },
  async leave(b) {
    const { c, room, pl } = await auth(b);
    await pipe([['HDEL', K(c, 'p'), pl.player_id]]);
    if (pl.is_host && room.status === 'waiting') {
      const [all] = await pipe([['HGETALL', K(c, 'p')]]);
      const ps = vals(all).sort((a, b) => a.t - b.t);
      if (ps[0]) { ps[0].is_host = true; room.host_name = ps[0].player_name; await saveP(c, ps[0]); await saveR(c, room); }
    }
    return { ok: 1 };
  },
  async start(b) {
    const { c, room, pl } = await auth(b);
    if (!pl.is_host) throw new E('Только капитан может начать игру', 403);
    const [all] = await pipe([['HGETALL', K(c, 'p')]]); const ps = vals(all);
    if (ps.length < 6 || !ps.every(p => p.is_ready)) throw new E('Нужно минимум 6 игроков и готовность каждого.');
    room.status = 'started'; await saveR(c, room); return { ok: 1 };
  },
  async views(b) { // капитан публикует персональные «виды» игры для каждого игрока
    const { c, pl } = await auth(b);
    if (!pl.is_host) throw new E('Только капитан', 403);
    const f = []; Object.entries(b.views || {}).forEach(([id, v]) => f.push(id, JSON.stringify(v)));
    if (f.length) await pipe([['HSET', K(c, 'v'), ...f], ['EXPIRE', K(c, 'v'), TTL]]);
    return { ok: 1 };
  },
  async msg(b) { // действие игрока (голос, выбор жертвы); отправителя ставит сервер
    const { c, pl } = await auth(b);
    const m = { ...(b.m || {}), from: pl.player_id };
    if (JSON.stringify(m).length > 500) throw new E('Слишком длинное сообщение');
    await pipe([['RPUSH', K(c, 'q'), JSON.stringify(m)], ['LTRIM', K(c, 'q'), -300, -1], ['EXPIRE', K(c, 'q'), TTL]]);
    return { ok: 1 };
  },
};
async function state(q) {
  const c = norm(q.code), pid = String(q.pid || '-');
  const [r, p, v] = await pipe([['GET', K(c, 'r')], ['HGET', K(c, 'p'), pid], ['HGET', K(c, 'v'), pid]]);
  if (!r) throw new E('Комната не найдена', 404);
  if (!p) throw new E('Игрок не найден', 403);
  const room = JSON.parse(r), pl = JSON.parse(p);
  if (pl.tok !== q.tok) throw new E('Нет доступа', 403);
  let players = [], msgs = [];
  if (room.status === 'waiting' || pl.is_host) { const [all] = await pipe([['HGETALL', K(c, 'p')]]); players = vals(all).sort((a, b) => a.t - b.t).map(pub); }
  if (room.status === 'started' && pl.is_host) { const [m] = await pipe([['LPOP', K(c, 'q'), 100]]); msgs = (m || []).map(x => JSON.parse(x)); }
  return { room, players, me: pub(pl), view: v ? JSON.parse(v) : null, msgs }; // игрок получает только свой «вид»
}
module.exports = async (req, res) => {
  res.setHeader('Cache-Control', 'no-store');
  try {
    if (!U || !T) return res.status(500).json({ error: 'Не подключена база Upstash Redis (см. README)' });
    if (req.method === 'GET') return res.status(200).json(await state(req.query));
    const b = req.body || {};
    if (!A[b.a]) throw new E('Неизвестное действие');
    return res.status(200).json(await A[b.a](b));
  } catch (e) {
    if (e.user) return res.status(e.status).json({ error: e.message });
    console.error(e);
    return res.status(500).json({ error: 'Ошибка сервера, попробуйте ещё раз' });
  }
};
