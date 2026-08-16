import { spawn } from 'node:child_process'
import { WebSocket } from 'ws'

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const results = []
const pass = (name) => { results.push(`PASS ${name}`); console.log(`✓ ${name}`) }
const fail = (name, err) => { results.push(`FAIL ${name}: ${err}`); console.log(`✗ ${name}: ${err}`) }

async function getJSON(url) {
  const res = await fetch(url)
  return res.json()
}

const child = spawn(process.execPath, ['start-all.js'], { cwd: process.cwd(), stdio: 'ignore' })

let ok = true
try {
  let up = false
  for (let i = 0; i < 40; i++) {
    try { await getJSON('http://localhost:5000/health'); up = true; break } catch { await sleep(300) }
  }
  if (!up) throw new Error('registry did not start')

  let services = []
  for (let i = 0; i < 40; i++) {
    const snap = await getJSON('http://localhost:5000/api/services')
    if (snap.services.length === 3) { services = snap.services; break }
    await sleep(300)
  }
  if (services.length === 3) pass('3 services registered with registry')
  else fail('3 services registered', JSON.stringify(services))
  const allOnline = services.every((s) => s.status === 'ONLINE' && s.port >= 5001 && s.port <= 5003)
  if (allOnline) pass(`services online on 5001-5003: ${services.map((s) => `${s.type}:${s.port}`).join(', ')}`)
  else fail('services online', JSON.stringify(services))

  const { nearest } = await getJSON('http://localhost:5000/api/nearest?lat=19.0839&lng=72.8718')
  const types = Object.keys(nearest).sort()
  if (types.join(',') === 'FIRE,HOSPITAL,POLICE' && nearest.POLICE.distanceKm > 0) {
    pass(`nearest lookup (haversine) → ${types.map((t) => `${t} ${nearest[t].distanceKm.toFixed(1)}km`).join(', ')}`)
  } else fail('nearest lookup', JSON.stringify(nearest))

  const user = new WebSocket('ws://localhost:5001')
  const dash = new WebSocket('ws://localhost:5001', { headers: { 'x-dashboard': '1' } })
  user.on('error', () => {})
  dash.on('error', () => {})
  await Promise.all([new Promise((r) => user.on('open', r)), new Promise((r) => dash.on('open', r))])

  const dashIncidents = []
  const userMsgs = []
  dash.on('message', (raw) => { const m = JSON.parse(raw); if (m.type === 'INCIDENT_UPDATE') dashIncidents.push(m.payload) })
  user.on('message', (raw) => userMsgs.push(JSON.parse(raw)))

  const incidentId = `ALRT-SMOKE-${Date.now().toString(36).toUpperCase()}`
  user.send(JSON.stringify({
    type: 'ALERT',
    payload: { incidentId, severity: 'HIGH', message: 'smoke test alert', lat: 19.0839, lng: 72.8718, sourceId: 'TEST' },
  }))

  await sleep(700)
  const inc = dashIncidents.find((i) => i.incidentId === incidentId)
  if (inc && inc.status === 'OPEN' && inc.severity === 'HIGH') pass('alert delivered to dashboard (OPEN)')
  else fail('alert delivered to dashboard', JSON.stringify(dashIncidents))

  dash.send(JSON.stringify({ type: 'ALERT_ACK', payload: { incidentId, eta: 5 } }))
  await sleep(500)
  const ack = userMsgs.find((m) => m.type === 'ALERT_ACK' && m.payload.incidentId === incidentId)
  if (ack && ack.payload.etaMinutes === 5) pass('dashboard ack relayed to user (ETA 5 min)')
  else fail('ack relayed to user', JSON.stringify(userMsgs))

  dash.send(JSON.stringify({ type: 'DISPATCH_UPDATE', payload: { incidentId, action: 'RESOLVED', note: 'handled' } }))
  await sleep(500)
  const res = userMsgs.find((m) => m.type === 'DISPATCH_UPDATE' && m.payload.incidentId === incidentId)
  if (res && res.payload.status === 'RESOLVED') pass('dispatch update relayed to user (RESOLVED)')
  else fail('dispatch update relayed', JSON.stringify(userMsgs))

  user.send(JSON.stringify({
    type: 'ALERT',
    payload: { incidentId, severity: 'LOW', message: 'duplicate test', lat: 19.0839, lng: 72.8718, sourceId: 'TEST' },
  }))
  await sleep(500)
  const dup = userMsgs.find((m) => m.type === 'ALERT_DUPLICATE' && m.payload.incidentId === incidentId)
  if (dup) pass('duplicate alert suppressed (ALERT_DUPLICATE)')
  else fail('duplicate suppression', JSON.stringify(userMsgs))

  const chatMsgs = []
  user.on('message', (raw) => { const m = JSON.parse(raw); if (m.type === 'CHAT') chatMsgs.push(m.payload) })
  user.send(JSON.stringify({ type: 'CHAT', payload: { from: 'TEST-USER', text: 'hello police' } }))
  await sleep(500)
  const echo = chatMsgs.find((c) => c.text === 'hello police')
  if (echo && echo.from === 'TEST-USER') pass('chat echo received')
  else fail('chat echo', JSON.stringify(chatMsgs))

  const after = await getJSON('http://localhost:5000/health')
  if (after.services === 3) pass('heartbeats keep all 3 services registered')
  else fail('heartbeats', JSON.stringify(after))

  user.close(); dash.close()
} catch (e) {
  fail('smoke run', e.message)
} finally {
  child.kill('SIGTERM')
  await sleep(500)
  try { child.kill('SIGKILL') } catch {}
}

console.log('---')
console.log(results.filter((r) => r.startsWith('PASS')).length, 'passed,', results.filter((r) => r.startsWith('FAIL')).length, 'failed')
process.exit(results.some((r) => r.startsWith('FAIL')) ? 1 : 0)