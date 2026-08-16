import http from 'http'

// Single-port gateway: exposes the whole emergency-services stack on ONE port so it
// deploys as a single service on Railway / Render / etc. Routes by path prefix:
//   /registry  → :5000   /police → :5001   /hospital → :5002   /fire → :5003
// Both HTTP (REST) and WebSocket upgrade requests are proxied to the internal servers.

const PORT = Number(process.env.PORT) || 8080

const ROUTES = [
  { prefix: '/registry', port: 5000, label: 'central registry' },
  { prefix: '/police', port: 5001, label: 'police service' },
  { prefix: '/hospital', port: 5002, label: 'hospital service' },
  { prefix: '/fire', port: 5003, label: 'fire service' },
]

function matchRoute(url) {
  let path = '/'
  try {
    path = new URL(url, 'http://localhost').pathname
  } catch {
    return null
  }
  for (const r of ROUTES) {
    if (path === r.prefix || path.startsWith(`${r.prefix}/`)) {
      return { ...r, path: path.slice(r.prefix.length) || '/' }
    }
  }
  return null
}

const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' })
    res.end(
      JSON.stringify({
        ok: true,
        name: 'emergency-services gateway',
        routes: ROUTES.map((r) => ({ path: r.prefix, targetPort: r.port, label: r.label })),
      })
    )
    return
  }

  const route = matchRoute(req.url)
  if (!route) {
    res.writeHead(404, { 'content-type': 'text/plain' })
    res.end('not found')
    return
  }

  const proxy = http.request(
    { host: '127.0.0.1', port: route.port, path: route.path, method: req.method, headers: { ...req.headers } },
    (pres) => {
      res.writeHead(pres.statusCode, pres.headers)
      pres.pipe(res)
    }
  )
  proxy.on('error', () => {
    if (!res.headersSent) res.writeHead(502, { 'content-type': 'text/plain' })
    res.end('upstream unavailable')
  })
  req.pipe(proxy)
})

server.on('upgrade', (req, socket, head) => {
  const route = matchRoute(req.url)
  if (!route) {
    socket.destroy()
    return
  }

  const proxy = http.request({
    host: '127.0.0.1',
    port: route.port,
    path: route.path,
    method: 'GET',
    headers: { ...req.headers },
  })

  proxy.on('upgrade', (pres, proxySocket, proxyHead) => {
    const lines = ['HTTP/1.1 101 Switching Protocols']
    for (const h of ['upgrade', 'connection', 'sec-websocket-accept', 'sec-websocket-protocol', 'sec-websocket-extensions']) {
      const v = pres.headers[h]
      if (v) lines.push(`${h}: ${v}`)
    }
    socket.write(lines.join('\r\n') + '\r\n\r\n')
    if (proxyHead && proxyHead.length) proxySocket.write(proxyHead)
    proxySocket.pipe(socket)
    socket.pipe(proxySocket)
  })

  proxy.on('error', () => socket.destroy())
  proxy.end()
})

server.listen(PORT, () => {
  console.log(`[GATEWAY] single-port gateway on http://localhost:${PORT}`)
  for (const r of ROUTES) console.log(`  ws / rest ${r.prefix} → :${r.port} (${r.label})`)
})