import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

const DATA_DIR = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'data')

function filePath(name) {
  return path.join(DATA_DIR, name)
}

export function loadJson(name, fallback) {
  try {
    const p = filePath(name)
    if (!fs.existsSync(p)) return fallback
    return JSON.parse(fs.readFileSync(p, 'utf8'))
  } catch (e) {
    console.error('[store] load failed', name, e.message)
    return fallback
  }
}

export function saveJson(name, data) {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true })
    const p = filePath(name)
    const tmp = `${p}.tmp`
    fs.writeFileSync(tmp, JSON.stringify(data, null, 2))
    fs.renameSync(tmp, p)
  } catch (e) {
    console.error('[store] save failed', name, e.message)
  }
}

export function debouncedSave(name, data, ms = 400) {
  let timer = null
  return () => {
    clearTimeout(timer)
    timer = setTimeout(() => saveJson(name, data()), ms)
  }
}
