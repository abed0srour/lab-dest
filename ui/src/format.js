const UNITS = ['B', 'KB', 'MB', 'GB', 'TB']

export function bytes(n) {
  if (n === null || n === undefined) return '–'
  let i = 0
  while (Math.abs(n) >= 1024 && i < UNITS.length - 1) {
    n /= 1024
    i += 1
  }
  return i === 0 ? `${n} B` : `${n.toFixed(n < 10 ? 2 : 1)} ${UNITS[i]}`
}

export const count = (n) => new Intl.NumberFormat('en').format(n)

export const when = (iso) =>
  new Date(iso).toLocaleString('en-GB', { dateStyle: 'medium', timeStyle: 'short' })

export const tagList = (tags) => Object.entries(tags || {}).map(([k, v]) => `${k}=${v}`)

export const redundancyLabel = (r) => (r === 'ec' ? 'Erasure coding 4 + 2' : 'Replication × 3')
