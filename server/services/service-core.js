import express from 'express'
import http from 'http'
import { WebSocketServer } from 'ws'
import { MSG, pack, parse } from '../shared/protocol.js'
import { haversineKm, formatDistanceKm } from '../shared/geo.js'
import { computeTrust, verifiedFromScore, trustLabel, TRUST } from '../shared/trust.js'
import { loadJson, debouncedSave } from '../shared/store.js'

const REGISTRY_URL = process.env.REGISTRY_URL || 'ws://localhost:5000'
const HEARTBEAT_MS = 5000
const ESCALATE_MS = { CRITICAL: 30000, HIGH: 60000, MEDIUM: 120000, LOW: 180000 }

// When several people SOS the same accident, fold the reports into ONE incident so
// dispatch boards don't flood. Merge rule: same scene (< MERGE_DISTANCE_KM) reported
// while the first case is still active (< MERGE_WINDOW_MS).
const MERGE_DISTANCE_KM = 0.4
const MERGE_WINDOW_MS = 5 * 60 * 1000
const SEV_RANK = { LOW: 0, MEDIUM: 1, HIGH: 2, CRITICAL: 3 }

const ACTIONS = {
  POLICE: { ack: 'Acknowledge', dispatch: 'Dispatch Unit', resolve: 'Mark Resolved' },
  HOSPITAL: { ack: 'Accept', dispatch: 'Send Ambulance', resolve: 'Mark Resolved' },
  FIRE: { ack: 'Accept', dispatch: 'Dispatch Truck', resolve: 'Mark Resolved' },
}

export function createService(config) {
  const { id, name, type, port, lat, lng, capacity } = config
  const saved = loadJson(`service-${id}.json`, null)
  const state = {
    status: 'ONLINE',
    currentLoad: 0,
    incidents: new Map(saved?.incidents ? saved.incidents.map((i) => [i.incidentId, i]) : []),
    chat: saved?.chat ?? [],
    log: saved?.log ?? [],
    dashboards: new Set(),
    userConnections: new Set(),
    sourceHistory: new Map(saved?.sourceHistory ?? []),
  }

  const persist = debouncedSave(`service-${id}.json`, () => ({
    incidents: [...state.incidents.values()],
    chat: state.chat,
    log: state.log,
    sourceHistory: [...state.sourceHistory.entries()],
  }))

  const log = (entry) => {
    const rec = { ...entry, ts: Date.now() }
    state.log.push(rec)
    if (state.log.length > 150) state.log.shift()
  }

  const sendDashboards = (payload) => {
    const raw = pack(MSG.INCIDENT_UPDATE, payload)
    for (const ws of state.dashboards) if (ws.readyState === ws.OPEN) ws.send(raw)
  }

  const recalcLoad = () => {
    state.currentLoad = [...state.incidents.values()].filter(i => i.status === 'OPEN' || i.status === 'ACKED' || i.status === 'DISPATCHING').length
    if (registryWs && registryWs.readyState === registryWs.OPEN) {
      registryWs.send(pack(MSG.HEARTBEAT, { id, status: state.status, currentLoad: state.currentLoad }))
    }
  }

  const pushIncident = (alert, userWs) => {
    const incidentId = alert.incidentId || `${id}-${Date.now().toString(36).toUpperCase()}`
    if (state.incidents.has(incidentId)) {
      const existing = state.incidents.get(incidentId)
      userWs?.send(pack(MSG.ALERT_DUPLICATE, { incidentId, reason: 'duplicate alert suppressed', duplicateOf: existing.status }))
      sendDashboards({ incidentId, status: 'DUPLICATE_SUPPRESSED' })
      log({ kind: 'DUPLICATE', incidentId })
      return
    }
    const source = alert.sourceId || 'unknown'

    let merged = null
    if (Number.isFinite(alert.lat) && Number.isFinite(alert.lng)) {
      for (const inc of state.incidents.values()) {
        if (inc.status === 'RESOLVED') continue
        if (Date.now() - inc.receivedAt > MERGE_WINDOW_MS) continue
        if (!Number.isFinite(inc.lat) || !Number.isFinite(inc.lng)) continue
        if (haversineKm(inc.lat, inc.lng, alert.lat, alert.lng) <= MERGE_DISTANCE_KM) {
          merged = inc
          break
        }
      }
    }
    if (merged) {
      const reporters = Array.from(new Set([...(merged.reporters || []), source]))
      if (SEV_RANK[alert.severity] > SEV_RANK[merged.severity]) merged.severity = alert.severity
      merged.reporters = reporters
      merged.reportCount = reporters.length
      merged.lastReportAt = Date.now()
      persist()
      recalcLoad()
      sendDashboards({ ...merged, ts: Date.now() })
      broadcastToUsers(
        pack(MSG.INCIDENT_MERGED, {
          incidentId: merged.incidentId,
          reports: merged.reportCount,
          reporters,
          severity: merged.severity,
          service: { id, name, type, port, lat, lng },
        })
      )
      userWs?.send(
        pack(MSG.ALERT_DUPLICATE, {
          incidentId,
          reason: 'merged into nearby incident',
          mergedInto: merged.incidentId,
          reports: merged.reportCount,
          reporters,
          severity: merged.severity,
        })
      )
      log({ kind: 'MERGED', incidentId, mergedInto: merged.incidentId, source, reportCount: merged.reportCount })
      return
    }

    const history = state.sourceHistory.get(source) || []
    const trustScore = computeTrust({ score: alert.trustScore, lowBattery: alert.lowBattery, testMode: alert.testMode, source, history })
    const verified = verifiedFromScore(trustScore)
    const incident = {
      incidentId,
      severity: alert.severity || 'MEDIUM',
      lat: alert.lat, lng: alert.lng,
      message: alert.message || 'Emergency alert',
      source,
      reporters: [source],
      reportCount: 1,
      trustScore,
      verified,
      medical: alert.medical || null,
      lowBattery: alert.lowBattery || false,
      testMode: alert.testMode || false,
      distanceKm: haversineKm(lat, lng, alert.lat, alert.lng),
      nearestStation: alert.nearestStation || null,
      receivedAt: Date.now(),
      status: 'OPEN',
      ackEta: null,
      dispatchNote: null,
      resolvedAt: null,
      notes: [],
      safety: null,
      evidence: alert.evidence || null,
      photos: [],
    }
    state.incidents.set(incidentId, incident)
    persist()
    recalcLoad()
    sendDashboards({ ...incident, ts: incident.receivedAt })
    broadcastToUsers(pack(MSG.ALERT, { incident: { ...incident }, service: { id, name, type, port, lat, lng } }))
    log({ kind: 'ALERT_RECEIVED', incidentId, source, trustScore, verified, reason: verified ? 'trust ok' : `below verified threshold (${TRUST.VERIFIED_MIN})` })
    if (!verified) log({ kind: 'TRUST_FLAG', incidentId, source, trustScore })

    const escalateAfter = ESCALATE_MS[incident.severity] || ESCALATE_MS.MEDIUM
    setTimeout(() => {
      const inc = state.incidents.get(incidentId)
      if (inc && inc.status === 'OPEN') {
        inc.status = 'ESCALATED'
        inc.escalatedAt = Date.now()
        sendDashboards({ ...inc, ts: Date.now() })
        broadcastToUsers(pack(MSG.INCIDENT_UPDATE, { incidentId, service: { id, name, type, port }, status: 'ESCALATED', severity: inc.severity }))
        log({ kind: 'ESCALATED', incidentId, reason: `no acknowledgement within ${escalateAfter / 1000}s` })
        // AUTO-DISPATCH the nearest unit once escalated so the incident is never left unhandled.
        setTimeout(() => {
          const cur = state.incidents.get(incidentId)
          if (cur && cur.status === 'ESCALATED') {
            cur.status = 'DISPATCHING'
            cur.dispatchNote = `auto-dispatched — no ack, nearest ${type} unit en route`
            cur.ackEta = cur.ackEta || Math.max(2, Math.round((cur.distanceKm || 2) * 1.6))
            recalcLoad()
            persist()
            sendDashboards({ ...cur, ts: Date.now() })
            broadcastToUsers(pack(MSG.DISPATCH_UPDATE, { incidentId, service: { id, name, type }, status: 'DISPATCHING', note: cur.dispatchNote }))
            log({ kind: 'AUTO_DISPATCH', incidentId, note: cur.dispatchNote })
          }
        }, 4000)
      }
    }, escalateAfter)
  }

  const broadcastToUsers = (raw) => {
    for (const ws of state.userConnections) if (ws.readyState === ws.OPEN) ws.send(raw)
  }

  // ---------- Registry client ----------
  let registryWs = null
  let registryRetries = 0
  let registryBeat = null

  const connectRegistry = () => {
    const ws = new WebSocket(REGISTRY_URL)
    registryWs = ws
    ws.onopen = () => {
      registryRetries = 0
      ws.send(pack(MSG.REGISTER, {
        id, name, type, port, lat, lng, capacity,
        status: state.status, currentLoad: state.currentLoad,
      }))
      log({ kind: 'REGISTERED_WITH_REGISTRY' })
      clearInterval(registryBeat)
      registryBeat = setInterval(() => {
        if (ws.readyState === ws.OPEN) {
          ws.send(pack(MSG.HEARTBEAT, { id, status: state.status, currentLoad: state.currentLoad, name, type, port, lat, lng, capacity }))
        }
      }, HEARTBEAT_MS)
    }
    ws.onerror = () => ws.close()
    ws.onclose = () => {
      clearInterval(registryBeat)
      const delay = Math.min(10000, 1000 * 2 ** registryRetries++)
      setTimeout(connectRegistry, delay)
    }
  }
  connectRegistry()

  // ---------- Service server ----------
  const app = express()
  const server = http.createServer(app)

  const handleMessage = (msg, userWs) => {
    const { type, payload } = msg

    if (type === MSG.CHAT) {
      const chatEntry = { from: payload.from || 'USER', text: payload.text, ts: Date.now() }
      state.chat.push(chatEntry)
      if (state.chat.length > 80) state.chat.shift()
      persist()
      broadcastToUsers(pack(MSG.CHAT, { ...chatEntry, service: { id, name, type } }))
      sendDashboards({ incidentId: 'chat', chat: chatEntry })
      return
    }

    if (type === MSG.ALERT_ACK) {
      const inc = state.incidents.get(payload.incidentId)
      if (!inc) return
      inc.status = 'ACKED'
      inc.ackEta = payload.eta || Math.round(inc.distanceKm * 2)
      inc.ackAt = Date.now()
      recalcLoad()
      persist()
      sendDashboards({ ...inc, ts: Date.now() })
      broadcastToUsers(pack(MSG.ALERT_ACK, { incidentId: inc.incidentId, service: { id, name, type }, etaMinutes: inc.ackEta, status: 'ACKED' }))
      log({ kind: 'ACKED', incidentId: inc.incidentId, eta: inc.ackEta })
      return
    }

    if (type === MSG.DISPATCH_UPDATE) {
      const inc = state.incidents.get(payload.incidentId)
      if (!inc) return
      const wasDispatched = inc.status === 'DISPATCHING'
      inc.status = payload.action === 'RESOLVED' ? 'RESOLVED' : 'DISPATCHING'
      inc.dispatchNote = payload.note || null
      if (inc.status === 'RESOLVED') {
        inc.resolvedAt = Date.now()
        recalcLoad()
        const history = state.sourceHistory.get(inc.source) || []
        history.push({ ts: Date.now(), resolved: wasDispatched })
        if (history.length > 20) history.shift()
        state.sourceHistory.set(inc.source, history)
        if (!wasDispatched) log({ kind: 'FALSE_ALARM_CANDIDATE', incidentId, source: inc.source })
      }
      persist()
      sendDashboards({ ...inc, ts: Date.now() })
      broadcastToUsers(pack(MSG.DISPATCH_UPDATE, { incidentId: inc.incidentId, service: { id, name, type }, status: inc.status, note: inc.dispatchNote }))
      log({ kind: payload.action === 'RESOLVED' ? 'RESOLVED' : 'DISPATCH', incidentId: inc.incidentId })
      return
    }

    if (type === MSG.ALERT) {
      if (!payload.lat || !payload.lng) return
      pushIncident(payload, userWs)
      return
    }

    if (type === MSG.ALERT_CANCEL) {
      const inc = state.incidents.get(payload.incidentId)
      if (!inc || inc.source !== payload.source) return
      inc.status = 'RESOLVED'
      inc.resolvedAt = Date.now()
      inc.dispatchNote = 'cancelled by caller — false alarm'
      inc.safety = 'SAFE'
      recalcLoad()
      persist()
      sendDashboards({ ...inc, ts: Date.now() })
      broadcastToUsers(pack(MSG.ALERT_CANCEL, { incidentId: inc.incidentId, source: inc.source, service: { id, name, type } }))
      log({ kind: 'CANCELLED', incidentId, source: inc.source, reason: 'caller false-alarm' })
      return
    }

    if (type === MSG.INCIDENT_NOTE) {
      const inc = state.incidents.get(payload.incidentId)
      if (!inc) return
      const note = { from: payload.from || 'USER', text: payload.text, ts: Date.now() }
      inc.notes = [...(inc.notes || []), note].slice(-30)
      persist()
      sendDashboards({ ...inc, ts: Date.now() })
      broadcastToUsers(pack(MSG.INCIDENT_NOTE, { incidentId: inc.incidentId, from: note.from, text: note.text, ts: note.ts, service: { id, name, type } }))
      log({ kind: 'INCIDENT_NOTE', incidentId, from: note.from })
      return
    }

    if (type === MSG.INCIDENT_PHOTO) {
      const inc = state.incidents.get(payload.incidentId)
      if (!inc) return
      const photo = {
        id: payload.id || `PH-${Date.now().toString(36).toUpperCase()}`,
        from: payload.from || 'UNIT',
        role: payload.role || 'RESPONDER',
        photo: payload.photo,
        caption: payload.caption || '',
        ts: Date.now(),
      }
      inc.photos = [...(inc.photos || []), photo].slice(-40)
      persist()
      sendDashboards({ ...inc, ts: Date.now() })
      broadcastToUsers(pack(MSG.INCIDENT_PHOTO, { ...photo, incidentId: inc.incidentId, service: { id, name, type } }))
      log({ kind: 'INCIDENT_PHOTO', incidentId, from: photo.from })
      return
    }

    if (type === MSG.INCIDENT_CHAT) {
      const entry = { from: payload.from || 'USER', text: payload.text, incidentId: payload.incidentId, ts: Date.now() }
      state.chat.push(entry)
      if (state.chat.length > 80) state.chat.shift()
      persist()
      broadcastToUsers(pack(MSG.INCIDENT_CHAT, { ...entry, service: { id, name, type } }))
      sendDashboards({ incidentId: payload.incidentId, chat: entry })
      return
    }

    if (type === MSG.SAFETY_UPDATE) {
      const inc = state.incidents.get(payload.incidentId)
      if (!inc) return
      inc.safety = payload.safe ? 'SAFE' : 'NEED_HELP'
      inc.safetyAt = Date.now()
      persist()
      sendDashboards({ ...inc, ts: Date.now() })
      broadcastToUsers(pack(MSG.SAFETY_UPDATE, { incidentId: inc.incidentId, safe: payload.safe, service: { id, name, type } }))
      log({ kind: 'SAFETY', incidentId, status: inc.safety })
      return
    }

    if (type === MSG.LOCATION_UPDATE) {
      state.liveLocation = { lat: payload.lat, lng: payload.lng, ts: Date.now() }
      log({ kind: 'LOCATION_UPDATE', lat: payload.lat, lng: payload.lng })
      return
    }
  }

  app.use(express.json())
  app.post('/api/actions', (req, res) => {
    const msg = req.body
    if (!msg || !msg.type) return res.status(400).json({ error: 'type required' })
    handleMessage(msg, null)
    res.json({ ok: true })
  })

  const wss = new WebSocketServer({ server })

  wss.on('connection', (ws, req) => {
    const isDashboard = !!req.headers['x-dashboard']
    if (isDashboard) state.dashboards.add(ws)
    else state.userConnections.add(ws)

    ws.on('message', (raw) => {
      const msg = parse(raw)
      if (!msg || !msg.type) return
      handleMessage(msg, ws)
    })

    ws.on('close', () => {
      state.dashboards.delete(ws)
      state.userConnections.delete(ws)
    })
  })

  app.get('/health', (_req, res) => res.json({ id, type, port, status: state.status, currentLoad: state.currentLoad, capacity, incidents: state.incidents.size }))
  app.get('/api/incidents', (_req, res) => res.json({ incidents: [...state.incidents.values()] }))

  app.get('/', (_req, res) => {
    res.type('html').send(dashboardHTML({ id, name, type, port, actions: ACTIONS[type], capacity }))
  })

  server.listen(port, () => {
    console.log(`[${type}] ${name} listening on http://localhost:${port} (registry: ${REGISTRY_URL})`)
  })

  return {
    state,
    close: () => { registryWs?.close(); server.close() },
  }
}

function dashboardHTML({ id, name, type, port, actions, capacity }) {
  const color = type === 'POLICE' ? '#0099ff' : type === 'HOSPITAL' ? '#00d47e' : '#EDB40B'
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>${name} Dashboard</title>
<style>
  :root{--bg:#0a0e1c;--surface:#12172b;--border:#232a3d;--text:#f0f2f5;--muted:#8b909e;--accent:${color};--gold:#EDB40B;--green:#00d47e;--red:#E10600}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;padding:24px}
  .head{display:flex;align-items:center;gap:12px;margin-bottom:20px;flex-wrap:wrap}
  .badge{width:12px;height:12px;border-radius:50%;background:var(--green);box-shadow:0 0 10px var(--green);animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
  h1{font-size:18px;letter-spacing:2px;color:var(--accent)}
  .sub{color:var(--muted);font-size:12px;font-family:monospace}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px}
  .card h2{font-size:11px;letter-spacing:1.5px;color:var(--muted);margin-bottom:12px;font-weight:600}
  .stats{display:flex;gap:16px;flex-wrap:wrap}
  .stat .v{font-size:22px;font-family:monospace;color:var(--accent)}
  .stat .l{font-size:10px;color:var(--muted);letter-spacing:1px}
  .inc{background:#0d1226;border:1px solid var(--border);border-radius:10px;padding:12px;margin-bottom:10px}
  .inc .row1{display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap}
  .inc .id{font-family:monospace;font-size:12px;color:var(--accent)}
  .sev{font-size:10px;font-weight:700;padding:2px 8px;border-radius:99px;letter-spacing:1px}
  .sev-CRITICAL{background:rgba(225,6,0,.15);color:var(--red);border:1px solid rgba(225,6,0,.4)}
  .sev-HIGH{background:rgba(255,138,0,.15);color:#ff8a00;border:1px solid rgba(255,138,0,.4)}
  .sev-MEDIUM{background:rgba(237,180,11,.15);color:var(--gold);border:1px solid rgba(237,180,11,.4)}
  .sev-LOW{background:rgba(0,212,126,.15);color:var(--green);border:1px solid rgba(0,212,126,.4)}
  .st{font-size:10px;padding:2px 8px;border-radius:99px;border:1px solid var(--border);color:var(--muted)}
  .st-ACKED,.st-DISPATCHING{border-color:var(--gold);color:var(--gold)}
  .st-RESOLVED{border-color:var(--green);color:var(--green)}
  .st-ESCALATED{border-color:var(--red);color:var(--red)}
  .trust-ok{border-color:var(--green);color:var(--green);background:rgba(0,212,126,.08)}
  .trust-flag{border-color:var(--gold);color:var(--gold);background:rgba(237,180,11,.08)}
  .inc .msg{color:var(--text);font-size:13px;margin-top:8px}
  .inc .meta{color:var(--muted);font-size:11px;margin-top:6px;font-family:monospace}
  .btns{display:flex;gap:8px;margin-top:10px;flex-wrap:wrap}
  button{border:none;border-radius:8px;padding:8px 14px;font-size:12px;font-weight:600;cursor:pointer;transition:.15s;color:#0a0e1c}
  button:disabled{opacity:.4;cursor:not-allowed}
  .ack{background:var(--accent)}
  .dispatch{background:var(--gold)}
  .resolve{background:var(--green)}
  .chat{display:flex;gap:8px;margin-top:8px}
  .chat input{flex:1;background:#0d1226;border:1px solid var(--border);border-radius:8px;color:var(--text);padding:8px 12px;font-size:12px;outline:none}
  .chat button{background:var(--accent)}
  .chatlog{font-family:monospace;font-size:11px;max-height:180px;overflow-y:auto;margin-top:8px}
  .chatlog div{padding:4px 0;border-bottom:1px solid #1a2033}
  .chatlog .from{color:var(--accent)}
  .log{font-family:monospace;font-size:11px;max-height:260px;overflow-y:auto;line-height:1.7}
  .log div{border-bottom:1px solid #1a2033}
  .log .t{color:var(--muted)}
  .empty{color:var(--muted);font-size:12px}
</style>
</head>
<body>
  <div class="head">
    <span class="badge" id="badge"></span>
    <div>
      <h1>${name} — ${type} SERVICE</h1>
      <div class="sub">port ${port} · registered at central registry :5000 · WS endpoint ws://localhost:${port}</div>
    </div>
  </div>
  <div class="grid">
    <div class="card">
      <h2>STATUS & CAPACITY</h2>
      <div class="stats">
        <div class="stat"><div class="v" id="load">0</div><div class="l">ACTIVE INCIDENTS</div></div>
        <div class="stat"><div class="v" id="cap">${capacity}</div><div class="l">CAPACITY</div></div>
        <div class="stat"><div class="v" id="statusTxt">ONLINE</div><div class="l">STATUS</div></div>
      </div>
    </div>
    <div class="card">
      <h2>INCIDENT QUEUE</h2>
      <div id="queue"><div class="empty">no incidents — alerts from the user client appear here</div></div>
    </div>
    <div class="card">
      <h2>RESPONSE CHAT</h2>
      <div class="chat"><input id="chatInput" placeholder="Reply to the requester…"/><button onclick="sendChat()">Send</button></div>
      <div id="chatlog" class="chatlog"></div>
    </div>
    <div class="card">
      <h2>LOCAL AUDIT LOG</h2>
      <div id="log" class="log">waiting…</div>
    </div>
  </div>
<script>
const ws=new WebSocket('ws://'+location.host,{headers:{'x-dashboard':'1'}})
ws.onopen=()=>{try{ws.send(JSON.stringify({type:'CHAT',payload:{from:'${name}',text:'dashboard connected'}}))}catch(e){}}
const q=document.getElementById('queue'),cl=document.getElementById('chatlog'),lg=document.getElementById('log')
const ACTIONS=${JSON.stringify(actions)}
const STYLE={OPEN:'',ACKED:'st-ACKED',DISPATCHING:'st-DISPATCHING',RESOLVED:'st-RESOLVED',ESCALATED:'st-ESCALATED',DUPLICATE_SUPPRESSED:''}
function renderQueue(){
  const incs=[...document.querySelectorAll('.inc')]
}
function incidentHTML(i){
  const trustCls=(i.verified)?'trust-ok':(i.verified===false?'trust-flag':'')
  const trustTxt=(i.verified?'VERIFIED':(i.verified===false?'UNVERIFIED':'—'))
  return '<div class="inc" id="inc-'+i.incidentId+'">'+
    '<div class="row1"><span class="id">'+i.incidentId+'</span><span class="sev sev-'+i.severity+'">'+i.severity+'</span><span class="st '+STYLE[i.status]+'">'+i.status+'</span><span class="st '+trustCls+'">'+trustTxt+'</span></div>'+
    '<div class="msg">'+(i.message||'').replace(/</g,'&lt;')+'</div>'+
    '<div class="meta">trust '+i.trustScore+' · source '+(i.source||'unknown')+' · distance '+i.distanceKm.toFixed(1)+'km · nearest '+(i.nearestStation||'—')+' · lat '+i.lat.toFixed(4)+', lng '+i.lng.toFixed(4)+(i.ackEta?' · ETA '+i.ackEta+' min':'')+'</div>'+
    '<div class="btns">'+
      '<button class="ack" onclick="act(\''+i.incidentId+'\',\'ACK\')" '+(i.status!=='OPEN'?'disabled':'')+'>'+ACTIONS.ack+'</button>'+
      '<button class="dispatch" onclick="act(\''+i.incidentId+'\',\'DISPATCH\')" '+(i.status==='RESOLVED'||i.status==='DISPATCHING'?'disabled':'')+'>'+ACTIONS.dispatch+'</button>'+
      '<button class="resolve" onclick="act(\''+i.incidentId+'\',\'RESOLVE\')" '+(i.status==='RESOLVED'?'disabled':'')+'>'+ACTIONS.resolve+'</button>'+
    '</div></div>'
}
function act(id,action){
  if(action==='ACK') ws.send(JSON.stringify({type:'ALERT_ACK',payload:{incidentId:id,eta:Math.round(Math.random()*6+3)}}))
  if(action==='DISPATCH') ws.send(JSON.stringify({type:'DISPATCH_UPDATE',payload:{incidentId:id,action:'DISPATCH',note:'unit en route'}}))
  if(action==='RESOLVE') ws.send(JSON.stringify({type:'DISPATCH_UPDATE',payload:{incidentId:id,action:'RESOLVED',note:'incident handled'}}))
}
ws.onmessage=(e)=>{
  const m=JSON.parse(e.data)
  if(m.type==='INCIDENT_UPDATE'){
    if(m.payload.incidentId==='chat'){
      const d=document.createElement('div');d.innerHTML='<span class="from">'+(m.payload.chat.from||'?')+'</span> '+m.payload.chat.text.replace(/</g,'&lt;')
      cl.prepend(d);return
    }
    if(m.payload.status==='DUPLICATE_SUPPRESSED'){return}
    const el=document.getElementById('inc-'+m.payload.incidentId)
    if(el) el.outerHTML=incidentHTML(m.payload)
    else if(m.payload.chat){}
    else q.prepend(document.createRange().createContextualFragment(incidentHTML(m.payload)))
    document.getElementById('load').textContent=document.querySelectorAll('.inc:not(:has(.st-RESOLVED))').length||0
  }
  if(m.type==='CHAT'){
    const d=document.createElement('div');d.innerHTML='<span class="from">'+(m.payload.from||'?')+'</span> '+m.payload.text.replace(/</g,'&lt;')
    cl.prepend(d)
  }
}
function sendChat(){
  const inp=document.getElementById('chatInput');if(!inp.value.trim())return
  ws.send(JSON.stringify({type:'CHAT',payload:{from:'${name}',text:inp.value}}));inp.value=''
}
setInterval(()=>{fetch('/health').then(r=>r.json()).then(h=>{
  document.getElementById('load').textContent=h.currentLoad
  document.getElementById('statusTxt').textContent=h.status
  document.getElementById('badge').style.background=h.status==='ONLINE'?'var(--green)':'var(--red)'
}).catch(()=>{document.getElementById('badge').style.background='var(--red)';document.getElementById('statusTxt').textContent='OFFLINE'})},4000)
</script>
</body>
</html>`
}