const http = require('http')
const path = require('path')
const crypto = require('crypto')

try { require('dotenv').config({ path: path.join(__dirname, '..', '.env') }) } catch {}

const SG_URL = process.env.SLOTSGATEWAY_API_URL
const SG_LOGIN = process.env.SLOTSGATEWAY_LOGIN
const SG_PASS = process.env.SLOTSGATEWAY_PASSWORD
const SG_SALT = process.env.SLOTSGATEWAY_SALT
const SUPABASE_URL = process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const PORT = process.env.PORT || process.env.SERVER_PORT || 3001
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '*').split(',')

function validateCallback(timestamp, key) {
  if (!SG_SALT || !timestamp || !key) return true
  const age = Math.abs(Date.now() / 1000 - parseInt(timestamp))
  if (age > 30) return false
  const expected = crypto.createHash('md5').update(timestamp + SG_SALT).digest('hex')
  return expected === key
}

const SB_HEADERS = {
  'apikey': SUPABASE_KEY,
  'Authorization': `Bearer ${SUPABASE_KEY}`,
  'Content-Type': 'application/json',
  'Prefer': 'return=representation',
}

async function sbGet(table, params) {
  const qs = new URLSearchParams(params).toString()
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}?${qs}`, { headers: SB_HEADERS })
  return res.json()
}

async function sbPatch(table, filter, body) {
  await fetch(`${SUPABASE_URL}/rest/v1/${table}?${filter}`, {
    method: 'PATCH',
    headers: SB_HEADERS,
    body: JSON.stringify(body),
  })
}

async function sbInsert(table, body) {
  await fetch(`${SUPABASE_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: SB_HEADERS,
    body: JSON.stringify(body),
  })
}

async function getBalance(playerId) {
  const data = await sbGet('profiles', { id: `eq.${playerId}`, select: 'balance' })
  return data?.[0]?.balance || 0
}

function toCents(v) { return Math.round(v * 100) }
function fromCents(v) { return parseInt(v) / 100 }

async function sgApi(body) {
  const res = await fetch(SG_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ api_login: SG_LOGIN, api_password: SG_PASS, ...body }),
  })
  return res.json()
}

// Savaşta spin sayacı: her round bittiğinde çağrılır
async function trackBattleSpin(playerId, gameId) {
  // Oyuncunun aktif battle'ını bul
  const players = await sbGet('battle_players', {
    user_id: `eq.${playerId}`,
    status: 'eq.playing',
    select: '*',
  })
  const bp = players?.[0]
  if (!bp) return

  // Battle room'u kontrol et
  const rooms = await sbGet('battle_rooms', {
    id: `eq.${bp.room_id}`,
    status: 'eq.playing',
    select: '*',
  })
  const room = rooms?.[0]
  if (!room || room.game_id_hash !== gameId) return

  const newSpinCount = (bp.spin_count || 0) + 1
  await sbPatch('battle_players', `id=eq.${bp.id}`, { spin_count: newSpinCount })

  console.log(`[Battle] Spin ${newSpinCount}/${room.spin_count} │ ${playerId.slice(0,8)}.. │ ${room.game_name}`)

  // Limit doldu → otomatik sıra bitir
  if (newSpinCount >= room.spin_count) {
    console.log(`[Battle] Spin limiti doldu, sıra otomatik bitiriliyor...`)
    try {
      // finishTurn mantığını burada direkt çalıştır
      const txns = await sbGet('sg_transactions', {
        player_id: `eq.${playerId}`,
        game_id: `eq.${room.game_id_hash}`,
        select: '*',
        order: 'created_at.desc',
        limit: room.spin_count * 2 + 10,
      })

      const bets = (txns || []).filter(t => t.action === 'bet')
      const wins = (txns || []).filter(t => t.action === 'win')
      const totalBet = Math.abs(bets.reduce((s, t) => s + parseFloat(t.amount), 0))
      const totalWin = wins.reduce((s, t) => s + parseFloat(t.amount), 0)
      const netResult = totalWin - totalBet

      await sbPatch('battle_players', `room_id=eq.${bp.room_id}&user_id=eq.${playerId}`, {
        status: 'finished',
        total_bet: totalBet,
        total_win: totalWin,
        net_result: netResult,
      })

      const nextTurn = room.current_turn + 1
      if (nextTurn <= room.max_players) {
        await sbPatch('battle_rooms', `id=eq.${bp.room_id}`, { current_turn: nextTurn })
        await sbPatch('battle_players', `room_id=eq.${bp.room_id}&turn_order=eq.${nextTurn}`, { status: 'playing' })
        console.log(`[Battle] Sıra geçti → turn ${nextTurn} │ net: $${netResult.toFixed(2)}`)
      } else {
        // Herkes bitti
        const allPlayers = await sbGet('battle_players', {
          room_id: `eq.${bp.room_id}`,
          select: '*',
          order: 'net_result.desc',
        })

        const winner = allPlayers[0]
        const totalPot = room.entry_fee * room.max_players
        const rankings = allPlayers.map((p, i) => ({
          user_id: p.user_id, username: p.username,
          net_result: p.net_result, total_bet: p.total_bet,
          total_win: p.total_win, rank: i + 1,
        }))

        const winnerBal = await getBalance(winner.user_id)
        await sbPatch('profiles', `id=eq.${winner.user_id}`, {
          balance: parseFloat((winnerBal + totalPot).toFixed(2)),
        })

        await sbInsert('transactions', {
          user_id: winner.user_id, amount: totalPot, type: 'game_win',
          description: `Savaş kazancı: ${room.game_name}`,
        })

        await sbInsert('battle_results', {
          room_id: bp.room_id, winner_id: winner.user_id,
          total_pot: totalPot, rankings: JSON.stringify(rankings),
        })

        await sbPatch('battle_rooms', `id=eq.${bp.room_id}`, {
          status: 'finished', finished_at: new Date().toISOString(),
        })

        console.log(`[Battle] BİTTİ │ Kazanan: ${winner.username} │ Havuz: $${totalPot}`)
      }
    } catch (err) {
      console.error('[Battle AutoFinish Error]', err.message)
    }
  }
}

function parseBody(req) {
  return new Promise((resolve) => {
    let d = ''
    req.on('data', c => d += c)
    req.on('end', () => { try { resolve(JSON.parse(d)) } catch { resolve({}) } })
  })
}

function getCorsOrigin(req) {
  const origin = req?.headers?.origin || ''
  if (ALLOWED_ORIGINS.includes('*')) return '*'
  if (ALLOWED_ORIGINS.includes(origin)) return origin
  return ALLOWED_ORIGINS[0] || '*'
}

function json(res, obj, status = 200, req = null) {
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': req ? getCorsOrigin(req) : '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Credentials': 'true',
  })
  res.end(JSON.stringify(obj))
}

const server = http.createServer(async (req, res) => {
  if (req.method === 'OPTIONS') return json(res, {}, 200, req)

  const url = req.url.split('?')[0]
  const query = Object.fromEntries(new URL(req.url, `http://localhost:${PORT}`).searchParams)

  try {
    if (url === '/health') {
      return json(res, { status: 'ok', sg: !!SG_URL })
    }

    // ─── SlotsGateway Callback ──────────────────────────────
    if (url === '/callback') {
      // Log EVERYTHING that comes in
      console.log(`[CB RAW] ${JSON.stringify(query)}`)

      const { action, username, amount, game_id, round_id, call_id, type, rb, gameplay_final, timestamp, key } = query
      const pid = username
      const amt = amount ? fromCents(amount) : 0

      if (!validateCallback(timestamp, key)) {
        console.log(`[CB] REJECTED: invalid key/timestamp`)
        return json(res, { error: 2, balance: 0 })
      }

      if (!pid) {
        console.log(`[CB] WARNING: no username in callback`)
        return json(res, { error: 0, balance: 0 })
      }

      const cbStart = Date.now()

      if (action === 'balance') {
        const bal = await getBalance(pid)
        console.log(`[CB] balance  │ ${pid.slice(0,8)}.. │ ₺${bal.toFixed(2)}`)
        json(res, { error: 0, balance: toCents(bal) })
        sbInsert('callback_logs', { action, player_id: pid, game_id, amount: amt, response_code: 0, response_time_ms: Date.now() - cbStart, raw_query: query }).catch(() => {})
        return
      }

      if (action === 'debit') {
        const bal = await getBalance(pid)

        // Freespin bonusu: bakiye kesilmez
        if (type === 'bonus_fs') {
          console.log(`[CB] debit-fs │ ${pid.slice(0,8)}.. │ ₺${amt.toFixed(2)} (bonus, bakiye korundu)`)
          return json(res, { error: 0, balance: toCents(bal) })
        }

        if (bal < amt) {
          console.log(`[CB] debit    │ ${pid.slice(0,8)}.. │ ₺${amt.toFixed(2)} │ YETERSIZ (₺${bal.toFixed(2)})`)
          return json(res, { error: 1, balance: toCents(bal) })
        }

        const newBal = parseFloat((bal - amt).toFixed(2))
        // Önce bakiye güncelle, hemen response dön, transaction kaydını arka planda yap
        await sbPatch('profiles', `id=eq.${pid}`, { balance: newBal })
        console.log(`[CB] debit    │ ${pid.slice(0,8)}.. │ -₺${amt.toFixed(2)} │ ₺${bal.toFixed(2)} → ₺${newBal.toFixed(2)} │ ${game_id} │ r:${round_id} │ t:${type}`)
        json(res, { error: 0, balance: toCents(newBal) })
        // Fire and forget
        sbInsert('sg_transactions', {
          call_id, player_id: pid, game_id, round_id, action: 'bet', amount: -amt,
        }).catch(() => {})
        return
      }

      if (action === 'credit') {
        const bal = await getBalance(pid)
        const isRollback = rb === '1'
        const newBal = parseFloat((bal + amt).toFixed(2))

        await sbPatch('profiles', `id=eq.${pid}`, { balance: newBal })
        const tag = isRollback ? 'refund' : 'credit'
        console.log(`[CB] ${tag.padEnd(6)} │ ${pid.slice(0,8)}.. │ +₺${amt.toFixed(2)} │ ₺${bal.toFixed(2)} → ₺${newBal.toFixed(2)} │ ${game_id} │ r:${round_id}${gameplay_final === '1' ? ' │ END' : ''}`)
        json(res, { error: 0, balance: toCents(newBal) })
        sbInsert('sg_transactions', {
          call_id, player_id: pid, game_id, round_id,
          action: isRollback ? 'refund' : 'win', amount: amt,
        }).catch(() => {})

        // Round bitti → spin sayacını güncelle
        if (gameplay_final === '1' && !isRollback) {
          trackBattleSpin(pid, game_id).catch(() => {})
        }
        return
      }

      console.log(`[CB] unknown: ${action}`)
      return json(res, { error: 0, balance: 0 })
    }

    // ─── API: Bakiye Sorgula ────────────────────────────────
    if (url === '/api/balance' && req.method === 'POST') {
      const body = await parseBody(req)
      const bal = await getBalance(body.userId)
      return json(res, { balance: bal })
    }

    // ─── API: Game List ─────────────────────────────────────
    if (url === '/api/sg/games') {
      const data = await sgApi({ method: 'getGameList', show_additional: true, show_systems: 0, list_type: 1, currency: 'USD' })
      console.log(`[Games] ${data.error === 0 ? (data.response?.length || 0) + ' loaded' : data.error}`)
      return json(res, data)
    }

    // ─── API: Create Player ─────────────────────────────────
    if (url === '/api/sg/create-player' && req.method === 'POST') {
      const body = await parseBody(req)
      const data = await sgApi({
        method: 'createPlayer',
        user_username: body.userId,
        user_password: body.userId,
        currency: 'USD',
      })
      console.log(`[Player] ${body.userId.slice(0,8)}..`, data.error === 0 ? 'OK' : JSON.stringify(data))
      return json(res, data)
    }

    // ─── API: Get Game (Real) ───────────────────────────────
    if (url === '/api/sg/get-game' && req.method === 'POST') {
      const body = await parseBody(req)
      const data = await sgApi({
        method: 'getGame',
        user_username: body.userId,
        user_password: body.userId,
        gameid: body.gameIdHash,
        currency: 'USD',
        lang: body.lang || 'tr',
        play_for_fun: 0,
        homeurl: 'https://slotsavaslari.com',
        cashierurl: 'https://slotsavaslari.com',
      })
      console.log(`[Game] ${body.gameIdHash}`, data.error === 0 ? 'OK' : JSON.stringify(data))
      return json(res, data)
    }

    // ─── API: Get Game Demo ─────────────────────────────────
    if (url === '/api/sg/get-game-demo' && req.method === 'POST') {
      const body = await parseBody(req)
      const data = await sgApi({
        method: 'getGameDemo',
        gameid: body.gameIdHash,
        currency: 'USD',
        lang: body.lang || 'tr',
        homeurl: 'https://slotsavaslari.com',
        cashierurl: 'https://slotsavaslari.com',
      })
      console.log(`[Demo] ${body.gameIdHash}`, data.error === 0 ? 'OK' : JSON.stringify(data))
      return json(res, data)
    }

    // ─── API: Add Free Rounds ───────────────────────────────
    if (url === '/api/sg/add-freerounds' && req.method === 'POST') {
      const body = await parseBody(req)
      const data = await sgApi({
        method: 'addFreeRounds',
        user_id: body.userId,
        game_id_hash: body.gameIdHash,
        freespins_count: body.count || 10,
        bet_level: body.betLevel || 1,
      })
      console.log(`[FreeRounds] ${body.gameIdHash}`, data.error === 0 ? 'OK' : JSON.stringify(data))
      return json(res, data)
    }

    // ─── BATTLE: Oda Listesi ──────────────────────────────
    if (url === '/api/battles') {
      const rooms = await sbGet('battle_rooms', {
        select: '*',
        status: 'in.(waiting,playing)',
        order: 'created_at.desc',
      })
      return json(res, rooms)
    }

    // ─── BATTLE: Tek Oda ────────────────────────────────
    if (url.startsWith('/api/battles/') && req.method === 'GET') {
      const roomId = url.split('/api/battles/')[1]
      const rooms = await sbGet('battle_rooms', { id: `eq.${roomId}`, select: '*' })
      const players = await sbGet('battle_players', {
        room_id: `eq.${roomId}`,
        select: '*',
        order: 'turn_order.asc',
      })
      if (!rooms?.[0]) return json(res, { error: 'Room not found' }, 404)
      return json(res, { room: rooms[0], players: players || [] })
    }

    // ─── BATTLE: Oda Oluştur ────────────────────────────
    if (url === '/api/battles/create' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, username, gameIdHash, gameName, gameImage, gameProvider, entryFee, spinCount, maxPlayers } = body

      const bal = await getBalance(userId)
      if (bal < entryFee) return json(res, { error: 'Yetersiz bakiye' }, 400)

      // Oda oluştur
      const roomRes = await fetch(`${SUPABASE_URL}/rest/v1/battle_rooms`, {
        method: 'POST',
        headers: { ...SB_HEADERS, 'Prefer': 'return=representation' },
        body: JSON.stringify({
          game_id_hash: gameIdHash,
          game_name: gameName,
          game_image: gameImage || null,
          game_provider: gameProvider || null,
          entry_fee: entryFee,
          spin_count: spinCount || 10,
          max_players: maxPlayers || 5,
          current_players: 1,
          created_by: userId,
        }),
      })
      const room = (await roomRes.json())?.[0]
      if (!room) return json(res, { error: 'Oda oluşturulamadı' }, 500)

      // Oluşturanı oyuncu olarak ekle
      await sbInsert('battle_players', {
        room_id: room.id,
        user_id: userId,
        username: username,
        turn_order: 1,
      })

      // Bakiye kes
      await sbPatch('profiles', `id=eq.${userId}`, { balance: parseFloat((bal - entryFee).toFixed(2)) })
      await sbInsert('transactions', {
        user_id: userId, amount: -entryFee, type: 'game_entry',
        description: `Savaş girişi: ${gameName}`,
      })

      console.log(`[Battle] Oda oluşturuldu: ${room.id.slice(0,8)}.. │ ${gameName} │ ₺${entryFee} │ ${username}`)
      return json(res, { room })
    }

    // ─── BATTLE: Odaya Katıl ────────────────────────────
    if (url === '/api/battles/join' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, username, roomId } = body

      const rooms = await sbGet('battle_rooms', { id: `eq.${roomId}`, select: '*' })
      const room = rooms?.[0]
      if (!room) return json(res, { error: 'Oda bulunamadı' }, 404)
      if (room.status !== 'waiting') return json(res, { error: 'Oda artık müsait değil' }, 400)
      if (room.current_players >= room.max_players) return json(res, { error: 'Oda dolu' }, 400)

      // Zaten odada mı?
      const existing = await sbGet('battle_players', { room_id: `eq.${roomId}`, user_id: `eq.${userId}`, select: 'id' })
      if (existing?.length > 0) return json(res, { error: 'Zaten bu odadasın' }, 400)

      const bal = await getBalance(userId)
      if (bal < room.entry_fee) return json(res, { error: 'Yetersiz bakiye' }, 400)

      const newCount = room.current_players + 1

      await sbInsert('battle_players', {
        room_id: roomId,
        user_id: userId,
        username: username,
        turn_order: newCount,
      })

      const update = { current_players: newCount }
      if (newCount >= room.max_players) {
        update.status = 'playing'
        update.started_at = new Date().toISOString()
        update.current_turn = 1
      }
      await sbPatch('battle_rooms', `id=eq.${roomId}`, update)

      // Bakiye kes
      await sbPatch('profiles', `id=eq.${userId}`, { balance: parseFloat((bal - room.entry_fee).toFixed(2)) })
      await sbInsert('transactions', {
        user_id: userId, amount: -room.entry_fee, type: 'game_entry',
        description: `Savaş girişi: ${room.game_name}`,
      })

      // Eğer oda doluysa, ilk oyuncuyu aktif yap
      if (newCount >= room.max_players) {
        await sbPatch('battle_players', `room_id=eq.${roomId}&turn_order=eq.1`, { status: 'playing' })
      }

      console.log(`[Battle] Katılım: ${roomId.slice(0,8)}.. │ ${username} │ ${newCount}/${room.max_players}${newCount >= room.max_players ? ' │ BAŞLADI' : ''}`)
      return json(res, { success: true, started: newCount >= room.max_players })
    }

    // ─── BATTLE: Odadan Ayrıl ───────────────────────────
    if (url === '/api/battles/leave' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, roomId } = body

      const rooms = await sbGet('battle_rooms', { id: `eq.${roomId}`, select: '*' })
      const room = rooms?.[0]
      if (!room || room.status !== 'waiting') return json(res, { error: 'Ayrılamazsın' }, 400)

      // Oyuncuyu sil
      await fetch(`${SUPABASE_URL}/rest/v1/battle_players?room_id=eq.${roomId}&user_id=eq.${userId}`, {
        method: 'DELETE',
        headers: SB_HEADERS,
      })

      await sbPatch('battle_rooms', `id=eq.${roomId}`, { current_players: Math.max(0, room.current_players - 1) })

      // Bakiye iade
      const bal = await getBalance(userId)
      await sbPatch('profiles', `id=eq.${userId}`, { balance: parseFloat((bal + room.entry_fee).toFixed(2)) })
      await sbInsert('transactions', {
        user_id: userId, amount: room.entry_fee, type: 'refund',
        description: `Savaş iadesi: ${room.game_name}`,
      })

      console.log(`[Battle] Ayrıldı: ${roomId.slice(0,8)}.. │ ${userId.slice(0,8)}..`)
      return json(res, { success: true })
    }

    // ─── BATTLE: Sırayı Bitir ───────────────────────────
    if (url === '/api/battles/finish-turn' && req.method === 'POST') {
      const body = await parseBody(req)
      const { roomId, userId } = body

      const rooms = await sbGet('battle_rooms', { id: `eq.${roomId}`, select: '*' })
      const room = rooms?.[0]
      if (!room || room.status !== 'playing') return json(res, { error: 'Oyun aktif değil' }, 400)

      // Oyuncunun spin verilerini sg_transactions'dan hesapla
      const txns = await sbGet('sg_transactions', {
        player_id: `eq.${userId}`,
        game_id: `eq.${room.game_id_hash}`,
        select: '*',
      })

      // Bu oda için olan transaction'ları filtrele (son N spin)
      const bets = (txns || []).filter(t => t.action === 'bet')
      const wins = (txns || []).filter(t => t.action === 'win')
      const totalBet = Math.abs(bets.reduce((s, t) => s + parseFloat(t.amount), 0))
      const totalWin = wins.reduce((s, t) => s + parseFloat(t.amount), 0)
      const netResult = totalWin - totalBet

      // Oyuncuyu güncelle
      await sbPatch('battle_players', `room_id=eq.${roomId}&user_id=eq.${userId}`, {
        status: 'finished',
        total_bet: totalBet,
        total_win: totalWin,
        net_result: netResult,
        spin_count: bets.length,
      })

      // Sıradaki oyuncuya geç
      const nextTurn = room.current_turn + 1
      if (nextTurn <= room.max_players) {
        await sbPatch('battle_rooms', `id=eq.${roomId}`, { current_turn: nextTurn })
        await sbPatch('battle_players', `room_id=eq.${roomId}&turn_order=eq.${nextTurn}`, { status: 'playing' })
        console.log(`[Battle] Sıra geçti: ${roomId.slice(0,8)}.. │ turn ${nextTurn} │ ${userId.slice(0,8)}.. bitti (net: ₺${netResult.toFixed(2)})`)
      } else {
        // Herkes bitti - sonuçları hesapla
        const allPlayers = await sbGet('battle_players', {
          room_id: `eq.${roomId}`,
          select: '*',
          order: 'net_result.desc',
        })

        const winner = allPlayers[0]
        const totalPot = room.entry_fee * room.max_players
        const rankings = allPlayers.map((p, i) => ({
          user_id: p.user_id,
          username: p.username,
          net_result: p.net_result,
          total_bet: p.total_bet,
          total_win: p.total_win,
          rank: i + 1,
        }))

        // Kazanana ödeme
        const winnerBal = await getBalance(winner.user_id)
        await sbPatch('profiles', `id=eq.${winner.user_id}`, {
          balance: parseFloat((winnerBal + totalPot).toFixed(2)),
          total_wins: (await sbGet('profiles', { id: `eq.${winner.user_id}`, select: 'total_wins' }))?.[0]?.total_wins + 1 || 1,
        })

        // Herkesin total_games artır
        for (const p of allPlayers) {
          const prof = (await sbGet('profiles', { id: `eq.${p.user_id}`, select: 'total_games' }))?.[0]
          if (prof) await sbPatch('profiles', `id=eq.${p.user_id}`, { total_games: (prof.total_games || 0) + 1 })
        }

        await sbInsert('transactions', {
          user_id: winner.user_id, amount: totalPot, type: 'game_win',
          description: `Savaş kazancı: ${room.game_name}`,
        })

        await sbInsert('battle_results', {
          room_id: roomId,
          winner_id: winner.user_id,
          total_pot: totalPot,
          rankings: JSON.stringify(rankings),
        })

        await sbPatch('battle_rooms', `id=eq.${roomId}`, {
          status: 'finished',
          finished_at: new Date().toISOString(),
        })

        console.log(`[Battle] BİTTİ: ${roomId.slice(0,8)}.. │ Kazanan: ${winner.username} │ Havuz: ₺${totalPot}`)
        return json(res, { finished: true, winner: winner.username, pot: totalPot, rankings })
      }

      return json(res, { success: true, nextTurn })
    }

    // ─── Favoriler ─────────────────────────────────────────
    if (url === '/api/favorites' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, gameIdHash, gameName, gameImage, gameProvider } = body
      await sbInsert('user_favorites', { user_id: userId, game_id_hash: gameIdHash, game_name: gameName, game_image: gameImage, game_provider: gameProvider })
      return json(res, { success: true })
    }

    if (url === '/api/favorites/remove' && req.method === 'POST') {
      const body = await parseBody(req)
      await fetch(`${SUPABASE_URL}/rest/v1/user_favorites?user_id=eq.${body.userId}&game_id_hash=eq.${body.gameIdHash}`, { method: 'DELETE', headers: SB_HEADERS })
      return json(res, { success: true })
    }

    if (url === '/api/favorites' && req.method === 'GET') {
      const userId = query.userId
      const data = await sbGet('user_favorites', { user_id: `eq.${userId}`, select: '*', order: 'created_at.desc' })
      return json(res, data || [])
    }

    // ─── Son Oynananlar ─────────────────────────────────────
    if (url === '/api/recently-played' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, gameIdHash, gameName, gameImage, gameProvider } = body
      await sbInsert('recently_played', { user_id: userId, game_id_hash: gameIdHash, game_name: gameName, game_image: gameImage, game_provider: gameProvider })
      return json(res, { success: true })
    }

    if (url === '/api/recently-played' && req.method === 'GET') {
      const data = await sbGet('recently_played', { user_id: `eq.${query.userId}`, select: '*', order: 'played_at.desc', limit: 20 })
      return json(res, data || [])
    }

    // ─── Günlük Bonus ───────────────────────────────────────
    if (url === '/api/daily-bonus' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId } = body
      const profiles = await sbGet('profiles', { id: `eq.${userId}`, select: 'balance,last_daily_bonus,daily_streak' })
      const profile = profiles?.[0]
      if (!profile) return json(res, { error: 'Kullanıcı bulunamadı' }, 404)

      const now = new Date()
      const lastBonus = profile.last_daily_bonus ? new Date(profile.last_daily_bonus) : null
      if (lastBonus) {
        const diffH = (now - lastBonus) / (1000 * 60 * 60)
        if (diffH < 20) return json(res, { error: 'Günlük bonus zaten alındı', next_bonus_hours: Math.ceil(24 - diffH) }, 400)
      }

      const isConsecutive = lastBonus && (now - lastBonus) / (1000 * 60 * 60) < 48
      const newStreak = isConsecutive ? (profile.daily_streak || 0) + 1 : 1
      const bonusTable = [25, 50, 75, 100, 150, 200, 300]
      const bonusAmount = bonusTable[Math.min(newStreak - 1, bonusTable.length - 1)]

      const newBal = parseFloat((profile.balance + bonusAmount).toFixed(2))
      await sbPatch('profiles', `id=eq.${userId}`, { balance: newBal, last_daily_bonus: now.toISOString(), daily_streak: newStreak })
      await sbInsert('daily_bonus_claims', { user_id: userId, day_streak: newStreak, bonus_amount: bonusAmount })
      await sbInsert('transactions', { user_id: userId, amount: bonusAmount, type: 'deposit', description: `Günlük bonus (${newStreak}. gün)` })

      // XP kazandır
      const levels = await sbGet('user_levels', { user_id: `eq.${userId}`, select: '*' })
      if (levels?.[0]) {
        const newXp = (levels[0].xp || 0) + 5
        const newLevel = Math.floor(newXp / 100) + 1
        await sbPatch('user_levels', `user_id=eq.${userId}`, { xp: newXp, level: newLevel, updated_at: now.toISOString() })
        await sbPatch('profiles', `id=eq.${userId}`, { xp: newXp, level: newLevel })
      } else {
        await sbInsert('user_levels', { user_id: userId, xp: 5, level: 1 })
      }

      console.log(`[Bonus] ${userId.slice(0,8)}.. │ Gün ${newStreak} │ +₺${bonusAmount}`)
      return json(res, { success: true, bonus_amount: bonusAmount, streak: newStreak, new_balance: newBal })
    }

    // ─── Referans Sistemi ───────────────────────────────────
    if (url === '/api/referral/apply' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, referralCode } = body
      const referrers = await sbGet('profiles', { referral_code: `eq.${referralCode}`, select: 'id,username,balance' })
      const referrer = referrers?.[0]
      if (!referrer) return json(res, { error: 'Geçersiz referans kodu' }, 400)
      if (referrer.id === userId) return json(res, { error: 'Kendi kodunuzu kullanamazsınız' }, 400)

      const existing = await sbGet('referrals', { referred_id: `eq.${userId}`, select: 'id' })
      if (existing?.length > 0) return json(res, { error: 'Zaten bir referans kodu kullandınız' }, 400)

      const bonusAmount = 50
      await sbInsert('referrals', { referrer_id: referrer.id, referred_id: userId, bonus_amount: bonusAmount, status: 'completed' })

      const userBal = await getBalance(userId)
      await sbPatch('profiles', `id=eq.${userId}`, { balance: parseFloat((userBal + bonusAmount).toFixed(2)), referred_by: referrer.id })
      await sbInsert('transactions', { user_id: userId, amount: bonusAmount, type: 'deposit', description: 'Referans bonusu (davetli)' })

      const refBal = await getBalance(referrer.id)
      await sbPatch('profiles', `id=eq.${referrer.id}`, { balance: parseFloat((refBal + bonusAmount).toFixed(2)) })
      await sbInsert('transactions', { user_id: referrer.id, amount: bonusAmount, type: 'deposit', description: `Referans bonusu (${userId.slice(0,8)}.. davet etti)` })

      console.log(`[Referral] ${userId.slice(0,8)}.. → ${referrer.username} │ +₺${bonusAmount} x2`)
      return json(res, { success: true, bonus: bonusAmount })
    }

    // ─── Profil Güncelleme ──────────────────────────────────
    if (url === '/api/profile/update' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, username, bio, phone, avatar_url, sound_enabled } = body
      const updates = {}
      if (username !== undefined) updates.username = username
      if (bio !== undefined) updates.bio = bio
      if (phone !== undefined) updates.phone = phone
      if (avatar_url !== undefined) updates.avatar_url = avatar_url
      if (sound_enabled !== undefined) updates.sound_enabled = sound_enabled
      await sbPatch('profiles', `id=eq.${userId}`, updates)
      return json(res, { success: true })
    }

    // ─── Kupon / Promo Code ─────────────────────────────────
    if (url === '/api/coupon/redeem' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userId, code } = body
      const promos = await sbGet('promo_codes', { code: `eq.${code}`, is_active: 'eq.true', select: '*' })
      const promo = promos?.[0]
      if (!promo) return json(res, { error: 'Geçersiz veya süresi dolmuş kupon' }, 400)

      if (promo.expires_at && new Date(promo.expires_at) < new Date()) {
        return json(res, { error: 'Kupon süresi dolmuş' }, 400)
      }
      if (promo.max_uses && promo.current_uses >= promo.max_uses) {
        return json(res, { error: 'Kupon kullanım limiti dolmuş' }, 400)
      }

      const existing = await sbGet('promo_redemptions', { user_id: `eq.${userId}`, promo_id: `eq.${promo.id}`, select: 'id' })
      if (existing?.length > 0) return json(res, { error: 'Bu kuponu zaten kullandınız' }, 400)

      const bal = await getBalance(userId)
      await sbPatch('profiles', `id=eq.${userId}`, { balance: parseFloat((bal + promo.amount).toFixed(2)) })
      await sbInsert('promo_redemptions', { user_id: userId, promo_id: promo.id })
      await sbPatch('promo_codes', `id=eq.${promo.id}`, { current_uses: (promo.current_uses || 0) + 1 })
      await sbInsert('transactions', { user_id: userId, amount: promo.amount, type: 'deposit', description: `Kupon: ${code}` })

      console.log(`[Coupon] ${userId.slice(0,8)}.. │ ${code} │ +₺${promo.amount}`)
      return json(res, { success: true, amount: promo.amount })
    }

    // ─── Başarım Kontrol ────────────────────────────────────
    if (url === '/api/achievements' && req.method === 'GET') {
      const userId = query.userId
      const [allAch, userAch] = await Promise.all([
        sbGet('achievements', { select: '*', order: 'category,requirement_value.asc' }),
        userId ? sbGet('user_achievements', { user_id: `eq.${userId}`, select: '*' }) : Promise.resolve([]),
      ])
      return json(res, { achievements: allAch || [], user_achievements: userAch || [] })
    }

    // ─── Savaş Simülasyonu ──────────────────────────────────
    if (url === '/api/admin/battle-simulate' && req.method === 'POST') {
      const body = await parseBody(req)
      const { players, spinCount, entryFee } = body
      const results = players.map(p => {
        const spins = Array.from({ length: spinCount }, () => {
          const r = Math.random()
          if (r < 0.01) return { multiplier: 50 + Math.random() * 450, type: 'mega' }
          if (r < 0.05) return { multiplier: 10 + Math.random() * 40, type: 'big' }
          if (r < 0.3) return { multiplier: 1 + Math.random() * 9, type: 'normal' }
          if (r < 0.6) return { multiplier: 0.1 + Math.random() * 0.9, type: 'small' }
          return { multiplier: 0, type: 'loss' }
        })
        const totalWin = spins.reduce((s, sp) => s + sp.multiplier * (entryFee / spinCount), 0)
        const totalBet = entryFee
        return { username: p, spins, totalWin: parseFloat(totalWin.toFixed(2)), totalBet, netResult: parseFloat((totalWin - totalBet).toFixed(2)) }
      })
      results.sort((a, b) => b.netResult - a.netResult)
      const totalPot = entryFee * players.length
      return json(res, { results, winner: results[0]?.username, totalPot })
    }

    // ─── Destek Ticket ────────────────────────────────────
    if (url === '/api/tickets' && req.method === 'GET') {
      const status = query.status || ''
      const params = { select: '*, user:profiles(username, email, avatar_url)', order: 'created_at.desc', limit: 100 }
      if (status) params.status = `eq.${status}`
      const data = await sbGet('support_tickets', params)
      return json(res, data || [])
    }

    if (url === '/api/tickets' && req.method === 'POST') {
      const body = await parseBody(req)
      await sbInsert('support_tickets', { user_id: body.userId, subject: body.subject, category: body.category || 'general', priority: body.priority || 'normal' })
      return json(res, { success: true })
    }

    if (url.match(/^\/api\/tickets\/[^/]+\/messages$/) && req.method === 'GET') {
      const ticketId = url.split('/')[3]
      const data = await sbGet('ticket_messages', { ticket_id: `eq.${ticketId}`, select: '*, sender:profiles(username, avatar_url)', order: 'created_at.asc' })
      return json(res, data || [])
    }

    if (url.match(/^\/api\/tickets\/[^/]+\/messages$/) && req.method === 'POST') {
      const ticketId = url.split('/')[3]
      const body = await parseBody(req)
      await sbInsert('ticket_messages', { ticket_id: ticketId, sender_id: body.senderId, sender_role: body.senderRole || 'user', message: body.message })
      await sbPatch('support_tickets', `id=eq.${ticketId}`, { updated_at: new Date().toISOString() })
      return json(res, { success: true })
    }

    if (url.match(/^\/api\/tickets\/[^/]+\/status$/) && req.method === 'POST') {
      const ticketId = url.split('/')[3]
      const body = await parseBody(req)
      const update = { status: body.status, updated_at: new Date().toISOString() }
      if (body.status === 'closed') update.closed_at = new Date().toISOString()
      if (body.assignedTo) update.assigned_to = body.assignedTo
      await sbPatch('support_tickets', `id=eq.${ticketId}`, update)
      return json(res, { success: true })
    }

    // ─── Toplu İşlemler ─────────────────────────────────
    if (url === '/api/admin/bulk-balance' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userIds, amount, adminId, description } = body
      let affected = 0
      for (const uid of userIds) {
        const bal = await getBalance(uid)
        await sbPatch('profiles', `id=eq.${uid}`, { balance: parseFloat((bal + amount).toFixed(2)) })
        await sbInsert('transactions', { user_id: uid, amount, type: 'deposit', description: description || 'Admin toplu yükleme' })
        affected++
      }
      await sbInsert('bulk_operations', { type: 'balance', description: `${affected} kullanıcıya ${amount} yüklendi`, affected_count: affected, performed_by: adminId })
      return json(res, { success: true, affected })
    }

    if (url === '/api/admin/bulk-ban' && req.method === 'POST') {
      const body = await parseBody(req)
      const { userIds, ban, reason, adminId } = body
      for (const uid of userIds) {
        await sbPatch('profiles', `id=eq.${uid}`, { is_banned: ban, ban_reason: ban ? reason : null, banned_at: ban ? new Date().toISOString() : null })
      }
      await sbInsert('bulk_operations', { type: ban ? 'ban' : 'unban', description: `${userIds.length} kullanıcı ${ban ? 'banlandı' : 'ban kaldırıldı'}`, affected_count: userIds.length, performed_by: adminId })
      return json(res, { success: true, affected: userIds.length })
    }

    // ─── Dışa Aktarma ───────────────────────────────────
    if (url === '/api/admin/export' && req.method === 'POST') {
      const body = await parseBody(req)
      const { table, filters } = body
      const allowed = ['profiles', 'transactions', 'battle_rooms', 'battle_results', 'support_tickets']
      if (!allowed.includes(table)) return json(res, { error: 'Geçersiz tablo' }, 400)
      const params = { select: '*', order: 'created_at.desc', limit: 5000 }
      if (filters) Object.assign(params, filters)
      const data = await sbGet(table, params)
      return json(res, data || [])
    }

    // ─── Turnuva ────────────────────────────────────────
    if (url === '/api/tournaments' && req.method === 'GET') {
      const data = await sbGet('tournaments', { select: '*', order: 'start_time.desc', limit: 50 })
      return json(res, data || [])
    }

    if (url === '/api/tournaments' && req.method === 'POST') {
      const body = await parseBody(req)
      const tourRes = await fetch(`${SUPABASE_URL}/rest/v1/tournaments`, {
        method: 'POST', headers: { ...SB_HEADERS, 'Prefer': 'return=representation' },
        body: JSON.stringify(body),
      })
      const tour = (await tourRes.json())?.[0]
      return json(res, { success: true, tournament: tour })
    }

    if (url === '/api/tournaments/join' && req.method === 'POST') {
      const body = await parseBody(req)
      const { tournamentId, userId, username } = body
      const tours = await sbGet('tournaments', { id: `eq.${tournamentId}`, select: '*' })
      const tour = tours?.[0]
      if (!tour) return json(res, { error: 'Turnuva bulunamadı' }, 404)
      if (tour.current_participants >= tour.max_participants) return json(res, { error: 'Turnuva dolu' }, 400)
      if (tour.entry_fee > 0) {
        const bal = await getBalance(userId)
        if (bal < tour.entry_fee) return json(res, { error: 'Yetersiz bakiye' }, 400)
        await sbPatch('profiles', `id=eq.${userId}`, { balance: parseFloat((bal - tour.entry_fee).toFixed(2)) })
        await sbInsert('transactions', { user_id: userId, amount: -tour.entry_fee, type: 'game_entry', description: `Turnuva girişi: ${tour.name}` })
      }
      await sbInsert('tournament_participants', { tournament_id: tournamentId, user_id: userId, username })
      await sbPatch('tournaments', `id=eq.${tournamentId}`, { current_participants: tour.current_participants + 1 })
      return json(res, { success: true })
    }

    // ─── CMS ────────────────────────────────────────────
    if (url === '/api/cms' && req.method === 'GET') {
      const slug = query.slug
      if (slug) {
        const data = await sbGet('cms_pages', { slug: `eq.${slug}`, select: '*' })
        return json(res, data?.[0] || null)
      }
      const data = await sbGet('cms_pages', { select: '*', order: 'sort_order.asc,created_at.desc' })
      return json(res, data || [])
    }

    // ─── Webhook Gönder ───────────────────────────────────
    if (url === '/api/admin/webhook/test' && req.method === 'POST') {
      const body = await parseBody(req)
      const { webhookUrl, platform, message } = body
      try {
        let payload
        if (platform === 'discord') {
          payload = { content: message || 'Slot Savaşları test bildirimi' }
        } else if (platform === 'telegram') {
          const parts = webhookUrl.split('/sendMessage?chat_id=')
          payload = { text: message || 'Slot Savaşları test bildirimi' }
        } else if (platform === 'slack') {
          payload = { text: message || 'Slot Savaşları test bildirimi' }
        } else {
          payload = { event: 'test', message: message || 'Slot Savaşları test bildirimi', timestamp: new Date().toISOString() }
        }
        const whRes = await fetch(webhookUrl, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        })
        const success = whRes.ok
        console.log(`[Webhook] Test → ${platform} │ ${success ? 'OK' : whRes.status}`)
        return json(res, { success, status: whRes.status })
      } catch (err) {
        return json(res, { success: false, error: err.message }, 500)
      }
    }

    // ─── Sistem Sağlık ──────────────────────────────────
    if (url === '/api/admin/health-snapshot' && req.method === 'GET') {
      const start = Date.now()
      try { await sbGet('profiles', { select: 'id', limit: 1 }) } catch {}
      const dbMs = Date.now() - start

      const sgStart = Date.now()
      let sgStatus = 'unknown'
      try {
        const sgRes = await fetch(SG_URL || 'http://invalid', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ api_login: SG_LOGIN, api_password: SG_PASS, method: 'getGameList', list_type: 1, currency: 'USD' }) })
        sgStatus = sgRes.ok ? 'ok' : 'error'
      } catch { sgStatus = 'down' }

      const activeUsers = await sbGet('profiles', { select: 'id', limit: 1 })
      const activeBattles = await sbGet('battle_rooms', { status: 'in.(waiting,playing)', select: 'id' })

      const mem = process.memoryUsage()
      const snapshot = {
        api_response_ms: Date.now() - start,
        db_response_ms: dbMs,
        active_users: activeUsers?.length || 0,
        active_battles: activeBattles?.length || 0,
        memory_usage_mb: parseFloat((mem.heapUsed / 1024 / 1024).toFixed(1)),
        sg_api_status: sgStatus,
        uptime_seconds: Math.floor(process.uptime()),
      }

      await sbInsert('system_health_snapshots', snapshot).catch(() => {})
      return json(res, snapshot)
    }

    if (url === '/api/admin/health-history' && req.method === 'GET') {
      const data = await sbGet('system_health_snapshots', { select: '*', order: 'created_at.desc', limit: 50 })
      return json(res, data || [])
    }

    // ─── Global Arama ───────────────────────────────────
    if (url === '/api/admin/search' && req.method === 'GET') {
      const q = query.q
      if (!q || q.length < 2) return json(res, { users: [], battles: [], transactions: [] })

      const [users, battles, txns] = await Promise.all([
        sbGet('profiles', { or: `(username.ilike.%${q}%,email.ilike.%${q}%)`, select: 'id,username,email,avatar_url,role,balance', limit: 10 }),
        sbGet('battle_rooms', { game_name: `ilike.%${q}%`, select: 'id,game_name,game_image,status,entry_fee,max_players', limit: 10, order: 'created_at.desc' }),
        sbGet('transactions', { description: `ilike.%${q}%`, select: 'id,user_id,amount,type,description,created_at', limit: 10, order: 'created_at.desc' }),
      ])
      return json(res, { users: users || [], battles: battles || [], transactions: txns || [] })
    }

    // ─── Segment Hesaplama ──────────────────────────────
    if (url === '/api/admin/segment-compute' && req.method === 'POST') {
      const body = await parseBody(req)
      const { segmentId, rules } = body
      const { data: allUsers } = await fetch(`${SUPABASE_URL}/rest/v1/profiles?select=id,username,balance,total_games,total_wins,vip_tier,fraud_flags,is_banned,created_at&limit=5000`, { headers: SB_HEADERS }).then(r => r.json()).then(d => ({ data: d }))

      let matched = allUsers || []
      for (const rule of (rules || [])) {
        matched = matched.filter(u => {
          const val = u[rule.field]
          switch (rule.op) {
            case 'eq': return val == rule.value
            case 'gt': return Number(val) > Number(rule.value)
            case 'lt': return Number(val) < Number(rule.value)
            case 'gte': return Number(val) >= Number(rule.value)
            case 'in': return (rule.value || []).includes(val)
            case 'within_days': {
              if (!val) return false
              const diff = (Date.now() - new Date(val).getTime()) / (1000 * 60 * 60 * 24)
              return diff <= Number(rule.value)
            }
            case 'older_than_days': {
              if (!val) return true
              const diff = (Date.now() - new Date(val).getTime()) / (1000 * 60 * 60 * 24)
              return diff > Number(rule.value)
            }
            default: return true
          }
        })
      }

      if (segmentId) {
        await sbPatch('player_segments', `id=eq.${segmentId}`, {
          cached_count: matched.length,
          cached_user_ids: JSON.stringify(matched.map(u => u.id).slice(0, 500)),
          last_computed: new Date().toISOString(),
        })
      }

      return json(res, { count: matched.length, users: matched.slice(0, 50) })
    }

    json(res, { error: 'not found' }, 404)
  } catch (err) {
    console.error('[Error]', err.message)
    json(res, { error: err.message }, 500)
  }
})

server.listen(PORT, async () => {
  let ip = 'unknown'
  try {
    const r = await fetch('https://api.ipify.org')
    ip = await r.text()
  } catch {}

  console.log('')
  console.log('  Slot Savaslari Backend')
  console.log(`  http://localhost:${PORT}`)
  console.log(`  SG: ${SG_URL ? 'OK' : 'NOT SET'}`)
  console.log(`  IP: ${ip}`)
  console.log('')
})

server.on('error', (err) => {
  if (err.code === 'EADDRINUSE') {
    console.error(`Port ${PORT} kullanımda. Önceki process'i kapatın.`)
  } else {
    console.error('Server error:', err.message)
  }
})

process.on('uncaughtException', (err) => {
  console.error('Uncaught:', err.message)
})
