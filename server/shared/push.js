import fs from 'fs'
import path from 'path'
import url from 'url'
import webpush from 'web-push'

// VAPID keys live in server/vapid-keys.json (committed so they survive Railway
// restarts — subscriptions stay valid across redeploys).
const KEY_FILE = path.join(path.dirname(url.fileURLToPath(import.meta.url)), '..', 'vapid-keys.json')

let keys = null
try {
  keys = JSON.parse(fs.readFileSync(KEY_FILE, 'utf8'))
} catch {
  keys = null
}

if (keys?.publicKey && keys?.privateKey) {
  webpush.setVapidDetails('mailto:ops@phantom-sos.local', keys.publicKey, keys.privateKey)
} else {
  console.warn('[PUSH] missing vapid-keys.json — push notifications disabled')
}

export const vapidPublicKey = keys?.publicKey ?? null

export function sendPush(subscription, { title, body, data }) {
  if (!keys?.publicKey || !subscription?.endpoint) return Promise.resolve(null)
  return webpush
    .sendNotification(subscription, JSON.stringify({ title, body, data }))
    .catch((err) => {
      const code = err?.statusCode
      // 404/410 = subscription dead — caller removes it.
      if (code === 404 || code === 410) return { expired: true }
      if (code === 413) return { dropped: true }
      console.warn('[PUSH] send failed', code, err?.body?.slice?.(0, 80) ?? err?.message)
      return { dropped: true }
    })
}

export function isValidSubscription(sub) {
  return !!(
    sub &&
    typeof sub.endpoint === 'string' &&
    sub.endpoint.startsWith('http') &&
    sub.keys &&
    typeof sub.keys.p256dh === 'string' &&
    typeof sub.keys.auth === 'string'
  )
}