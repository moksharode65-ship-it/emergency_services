import express from 'express'
import http from 'http'
import { WebSocketServer } from 'ws'
import cors from 'cors'
import { MSG, SEVERITY, pack, parse } from './shared/protocol.js'
import { nearestOfEachType, formatDistanceKm } from './shared/geo.js'
import { loadJson, debouncedSave } from './shared/store.js'

const PORT = 5000
const HEARTBEAT_TIMEOUT_MS = 15000
const PRUNE_INTERVAL_MS = 5000

const services = new Map()
const saved = loadJson('registry.json', null)
const auditLog = saved?.auditLog ?? []
const incidentLog = saved?.incidentLog ?? []
const clients = new Set()
const devices = new Map() // nodeId → { nodeId, name, lat, lng, ws, lastSeen } — online citizen devices

const persist = debouncedSave('registry.json', () => ({
  auditLog: auditLog.slice(-200),
  incidentLog: incidentLog.slice(-200),
}))

function logAudit(entry) {
  const rec = { ...entry, ts: Date.now() }
  auditLog.push(rec)
  if (auditLog.length > 200) auditLog.shift()
  persist()
  broadcast(pack(MSG.LOG, rec))
}

function broadcast(payload) {
  for (const ws of clients) {
    if (ws.readyState === ws.OPEN) ws.send(payload)
  }
}

function snapshot() {
  return [...services.values()].map(({ ws, ...rest }) => ({ ...rest, uptime: uptimePct(rest) }))
}

function publishServices() {
  broadcast(pack(MSG.SERVICES, { services: snapshot() }))
}

function deviceSnapshot() {
  return [...devices.values()].map(({ ws, ...rest }) => ({ ...rest }))
}

function publishDevices() {
  broadcast(pack(MSG.DEVICES, { devices: deviceSnapshot() }))
}

function upsertDevice(ws, payload) {
  const nodeId = payload.nodeId
  if (!nodeId) return
  devices.set(nodeId, {
    nodeId,
    name: payload.name || nodeId,
    lat: Number.isFinite(payload.lat) ? payload.lat : 0,
    lng: Number.isFinite(payload.lng) ? payload.lng : 0,
    ws,
    lastSeen: Date.now(),
  })
  publishDevices()
}

function registerService(svc, ws) {
  const id = svc.id
  const existing = services.get(id)
  services.set(id, { ...svc, ws, lastSeen: Date.now(), firstSeen: existing ? existing.firstSeen : Date.now(), beatCount: existing ? existing.beatCount : 0 })
  logAudit({ kind: 'REGISTER', id, name: svc.name, type: svc.type, port: svc.port })
  if (existing) logAudit({ kind: 'RE_REGISTER', id, name: svc.name })
  publishServices()
  return { ok: true, id }
}

function uptimePct(svc) {
  if (!svc || !svc.firstSeen) return 100
  const elapsed = Math.max(5000, Date.now() - svc.firstSeen)
  const expected = Math.floor(elapsed / 5000)
  if (expected <= 0) return 100
  return Math.min(100, Math.round((svc.beatCount / expected) * 100))
}

function pruneStale() {
  const now = Date.now()
  let changed = false
  for (const [id, svc] of services) {
    if (now - svc.lastSeen > HEARTBEAT_TIMEOUT_MS) {
      services.delete(id)
      logAudit({ kind: 'OFFLINE', id, name: svc.name, reason: 'heartbeat timeout' })
      changed = true
    }
  }
  if (changed) publishServices()
  let devicesChanged = false
  for (const [nodeId, d] of devices) {
    if (now - d.lastSeen > HEARTBEAT_TIMEOUT_MS) {
      devices.delete(nodeId)
      devicesChanged = true
    }
  }
  if (devicesChanged) publishDevices()
}

const app = express()
app.use(cors())
app.use(express.json())

app.get('/api/services', (_req, res) => {
  res.json({ services: snapshot(), updatedAt: Date.now() })
})

app.get('/api/nearest', (req, res) => {
  const lat = parseFloat(req.query.lat)
  const lng = parseFloat(req.query.lng)
  if (Number.isNaN(lat) || Number.isNaN(lng)) {
    return res.status(400).json({ error: 'lat/lng required' })
  }
  const result = nearestOfEachType(snapshot(), lat, lng)
  logAudit({ kind: 'NEAREST_QUERY', lat, lng, result: Object.values(result).map(r => r.id) })
  res.json({ nearest: result })
})

app.get('/api/log', (_req, res) => res.json({ log: auditLog.slice(-100) }))
app.get('/api/incidents', (_req, res) => res.json({ incidents: incidentLog.slice(-100) }))

app.get('/health', (_req, res) => {
  const list = snapshot()
  res.json({
    ok: true,
    services: list.length,
    detail: list.map((s) => ({ id: s.id, type: s.type, port: s.port, status: s.status, lastSeenAgoMs: Date.now() - (s.lastSeen || 0) })),
  })
})

app.get('/', (_req, res) => {
  res.type('html').send(adminDashboardHTML())
})

const server = http.createServer(app)
const wss = new WebSocketServer({ server })

wss.on('connection', (ws) => {
  clients.add(ws)
  ws.send(pack(MSG.SERVICES, { services: snapshot() }))
  logAudit({ kind: 'CLIENT_CONNECT' })

  ws.on('message', (raw) => {
    const msg = parse(raw)
    if (!msg || !msg.type) return
    const { type, payload } = msg

    switch (type) {
      case MSG.REGISTER:
        registerService(payload, ws)
        break
      case MSG.DEREGISTER:
        if (services.delete(payload.id)) {
          logAudit({ kind: 'DEREGISTER', id: payload.id })
          publishServices()
        }
        break
      case MSG.HEARTBEAT: {
        const payloadFull = payload.id && payload.name
        let svc = services.get(payload.id)
        if (!svc && payloadFull) {
          svc = registerService(payload, ws)
          logAudit({ kind: 'HEARTBEAT_RE_REGISTER', id: payload.id, reason: 'registry restarted' })
        }
        if (svc) {
          svc.lastSeen = Date.now()
          svc.beatCount = (svc.beatCount || 0) + 1
          if (payload.status) svc.status = payload.status
          if (typeof payload.currentLoad === 'number') svc.currentLoad = payload.currentLoad
        }
        break
      }
      case MSG.NEAREST: {
        const result = nearestOfEachType(snapshot(), payload.lat, payload.lng)
        logAudit({ kind: 'NEAREST_QUERY', lat: payload.lat, lng: payload.lng })
        ws.send(pack(MSG.NEAREST_RESULT, { nearest: result }))
        break
      }
      case MSG.INCIDENT_UPDATE:
        incidentLog.push({ ...payload, ts: Date.now() })
        if (incidentLog.length > 200) incidentLog.shift()
        persist()
        broadcast(pack(MSG.INCIDENT_UPDATE, payload))
        break
      case MSG.DEVICE_JOIN:
        upsertDevice(ws, payload)
        logAudit({ kind: 'DEVICE_JOIN', id: payload.nodeId, name: payload.name })
        break
      case MSG.DEVICE_UPDATE:
        upsertDevice(ws, payload)
        break
      case MSG.MESH_SEND: {
        const target = devices.get(payload.to)
        if (target && target.ws.readyState === target.ws.OPEN) {
          target.ws.send(pack(MSG.MESH_MSG, payload))
        }
        break
      }
      default:
        break
    }
  })

  ws.on('close', () => {
    clients.delete(ws)
    logAudit({ kind: 'CLIENT_DISCONNECT' })
    let changed = false
    for (const [nodeId, d] of devices) {
      if (d.ws === ws) {
        devices.delete(nodeId)
        changed = true
      }
    }
    if (changed) publishDevices()
  })
})

setInterval(pruneStale, PRUNE_INTERVAL_MS)

server.listen(PORT, () => {
  console.log(`[REGISTRY] listening on http://localhost:${PORT}`)
})

function adminDashboardHTML() {
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Central Registry — Admin</title>
<style>
  :root{--bg:#0a0e1c;--surface:#12172b;--border:#232a3d;--text:#f0f2f5;--muted:#8b909e;--gold:#EDB40B;--green:#00d47e;--red:#E10600;--blue:#0099ff}
  *{box-sizing:border-box;margin:0;padding:0}
  body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;padding:24px}
  h1{font-size:18px;letter-spacing:2px;color:var(--gold);margin-bottom:4px}
  .sub{color:var(--muted);font-size:12px;margin-bottom:24px;font-family:monospace}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:16px}
  .card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:16px}
  .card h2{font-size:11px;letter-spacing:1.5px;color:var(--muted);margin-bottom:12px;font-weight:600}
  .svc{display:flex;align-items:center;gap:10px;padding:10px 0;border-bottom:1px solid var(--border)}
  .svc:last-child{border-bottom:none}
  .dot{width:10px;height:10px;border-radius:50%;flex:none}
  .online{background:var(--green);box-shadow:0 0 8px var(--green)}
  .offline{background:var(--red)}
  .svc .name{font-weight:600;font-size:14px}
  .svc .meta{color:var(--muted);font-size:11px;font-family:monospace}
  .svc .right{margin-left:auto;text-align:right}
  .load{font-size:12px;font-family:monospace}
  .bar{height:4px;background:var(--border);border-radius:2px;margin-top:4px;width:110px;overflow:hidden}
  .bar>div{height:100%;background:var(--gold)}
  .log{font-family:monospace;font-size:11px;line-height:1.7;max-height:340px;overflow-y:auto}
  .log div{border-bottom:1px solid #1a2033}
  .log .t{color:var(--muted)}
  .log .gold{color:var(--gold)}
  .log .green{color:var(--green)}
  .log .red{color:var(--red)}
  .inc{font-family:monospace;font-size:11px;line-height:1.7;max-height:340px;overflow-y:auto}
  .inc div{border-bottom:1px solid #1a2033;padding:6px 0}
  .sev-CRITICAL{color:var(--red)} .sev-HIGH{color:#ff8a00} .sev-MEDIUM{color:var(--gold)} .sev-LOW{color:var(--green)}
</style>
</head>
<body>
  <h1>CENTRAL REGISTRY</h1>
  <div class="sub">port 5000 · heartbeat timeout 15s · nearest-match via Haversine + load tie-break</div>
  <div class="grid">
    <div class="card"><h2>SERVICES</h2><div id="services">connecting…</div></div>
    <div class="card"><h2>INCIDENTS</h2><div id="incidents" class="inc">none</div></div>
    <div class="card"><h2>AUDIT LOG</h2><div id="log" class="log">waiting…</div></div>
  </div>
<script>
const TYPE_COLORS={POLICE:'#0099ff',HOSPITAL:'#00d47e',FIRE:'#EDB40B'}
const ws=new WebSocket('ws://'+location.host)
const svcEl=document.getElementById('services'), logEl=document.getElementById('log'), incEl=document.getElementById('incidents')
function renderServices(list){
  svcEl.innerHTML=''
  if(!list.length){svcEl.innerHTML='<div style="color:var(--muted);font-size:12px">no services registered</div>';return}
  for(const s of list){
    const on=s.status==='ONLINE'
    const div=document.createElement('div');div.className='svc'
    div.innerHTML='<span class="dot '+(on?'online':'offline')+'"></span>'+
      '<div><div class="name">'+s.name+'</div><div class="meta">'+s.type+' · :'+s.port+' · '+s.lat.toFixed(4)+','+s.lng.toFixed(4)+' · cap '+s.capacity+' · uptime '+(s.uptime!=null?s.uptime:'100')+'%</div></div>'+
      '<div class="right"><div class="load">'+(on?('load '+s.currentLoad+'/'+s.capacity):'OFFLINE')+'</div>'+
      (on?'<div class="bar"><div style="width:'+Math.min(100,s.currentLoad/s.capacity*100)+'%"></div></div>':'')+'</div>'
    svcEl.appendChild(div)
  }
}
ws.onmessage=(e)=>{
  const m=JSON.parse(e.data)
  if(m.type==='SERVICES') renderServices(m.payload.services)
  if(m.type==='LOG'){
    const d=document.createElement('div')
    const kindCls=m.payload.kind==='REGISTER'||m.payload.kind==='RE_REGISTER'?'gold':m.payload.kind==='OFFLINE'||m.payload.kind==='DEREGISTER'?'red':''
    d.innerHTML='<span class="t">'+new Date(m.payload.ts).toLocaleTimeString()+'</span> <span class="'+kindCls+'">'+m.payload.kind+'</span> '+m.payload.id+' '+(m.payload.name||'')
    logEl.prepend(d)
  }
  if(m.type==='INCIDENT_UPDATE'){
    const d=document.createElement('div')
    d.innerHTML='<span class="t">'+new Date(m.payload.ts||Date.now()).toLocaleTimeString()+'</span> <span class="sev-'+m.payload.severity+'">'+m.payload.severity+'</span> '+m.payload.incidentId+' → '+m.payload.status
    incEl.prepend(d)
  }
}
ws.onopen=()=>{logEl.innerHTML='<div><span class="t">live</span> connected to registry</div>'}
</script>
</body>
</html>`
}