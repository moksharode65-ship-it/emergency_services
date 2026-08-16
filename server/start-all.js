import { spawn } from 'node:child_process'

const procs = [
  { name: 'registry', port: 5000, cmd: ['node', 'registry.js'] },
  { name: 'police', port: 5001, cmd: ['node', 'services/police.js'] },
  { name: 'hospital', port: 5002, cmd: ['node', 'services/hospital.js'] },
  { name: 'fire', port: 5003, cmd: ['node', 'services/fire.js'] },
]

const children = []
for (const p of procs) {
  const child = spawn(process.execPath, p.cmd.slice(1), { cwd: process.cwd(), stdio: 'inherit' })
  child.on('exit', (code) => {
    console.log(`[${p.name}] exited with code ${code}`)
    if (code !== 0 && code !== null) process.exit(code)
  })
  children.push(child)
}

process.on('SIGINT', () => { for (const c of children) c.kill('SIGINT'); process.exit(0) })
process.on('SIGTERM', () => { for (const c of children) c.kill('SIGTERM'); process.exit(0) })

console.log('started: registry :5000 · police :5001 · hospital :5002 · fire :5003')