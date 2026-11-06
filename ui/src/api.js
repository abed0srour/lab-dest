// Every call goes to /api, which nginx (Docker) or Vite (npm run dev) forwards to the gateway.
const BASE = '/api'

export const getKey = () => localStorage.getItem('apiKey') || 'student-key-2026'
export const setKey = (key) => localStorage.setItem('apiKey', key)

// Keys may contain "/" (folders): encode each part, keep the slashes.
export const encodeKey = (key) => key.split('/').map(encodeURIComponent).join('/')

function query(params) {
  const q = new URLSearchParams()
  for (const [k, v] of Object.entries(params || {})) {
    if (v === undefined || v === null || v === '') continue
    if (Array.isArray(v)) v.forEach((x) => q.append(k, x))
    else q.append(k, v)
  }
  const s = q.toString()
  return s ? `?${s}` : ''
}

async function failure(status, read) {
  let detail
  try {
    const body = JSON.parse(await read())
    detail = typeof body.detail === 'string' ? body.detail : JSON.stringify(body.detail)
  } catch {
    detail = `HTTP ${status}`
  }
  const e = new Error(detail)
  e.status = status
  return e
}

export async function call(path, { method = 'GET', params, auth = true } = {}) {
  const r = await fetch(BASE + path + query(params), { method, headers: auth ? { 'X-API-Key': getKey() } : {} })
  if (!r.ok) throw await failure(r.status, () => r.text())
  return r.status === 204 ? null : r.json()
}

const objectPath = (bucket, key) => `/buckets/${bucket}/objects/${encodeKey(key)}`

export const health = () => call('/health', { auth: false })
export const listBuckets = () => call('/buckets')
export const createBucket = (name, redundancy) => call(`/buckets/${name}`, { method: 'PUT', params: { redundancy } })
export const listObjects = (bucket, prefix) => call(`/buckets/${bucket}/objects`, { params: { prefix } })
export const objectMetadata = (bucket, key) => call(objectPath(bucket, key), { params: { metadata: true } })
export const deleteObject = (bucket, key) => call(objectPath(bucket, key), { method: 'DELETE' })
export const search = (params) => call('/search', { params })
export const searchStatus = () => call('/search/status')
export const clusterStatus = () => call('/cluster/status')
export const repairStats = () => call('/cluster/repairs')
export const scrubNow = () => call('/cluster/scrub', { method: 'POST' })

// XMLHttpRequest, not fetch: only XHR reports upload progress. The gateway streams the body into
// the cluster as it arrives, so this progress is also the storage progress.
export function uploadFile(bucket, key, file, { tags, chunking }, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest()
    xhr.open('PUT', BASE + objectPath(bucket, key) + query({ chunking }))
    xhr.setRequestHeader('X-API-Key', getKey())
    xhr.setRequestHeader('Content-Type', file.type || 'application/octet-stream')
    if (tags) xhr.setRequestHeader('X-Tags', tags)
    xhr.upload.onprogress = (e) => e.lengthComputable && onProgress(e.loaded, e.total)
    xhr.onload = async () => {
      if (xhr.status >= 200 && xhr.status < 300) resolve(JSON.parse(xhr.responseText))
      else reject(await failure(xhr.status, async () => xhr.responseText))
    }
    xhr.onerror = () => reject(new Error('network error'))
    xhr.send(file)
  })
}

// Read the download as a stream to show progress, then hand it to the browser as a file.
export async function downloadObject(bucket, key, size, onProgress) {
  const r = await fetch(BASE + objectPath(bucket, key), { headers: { 'X-API-Key': getKey() } })
  if (!r.ok) throw await failure(r.status, () => r.text())
  const reader = r.body.getReader()
  const parts = []
  let received = 0
  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    parts.push(value)
    received += value.length
    onProgress(received, size)
  }
  const url = URL.createObjectURL(new Blob(parts, { type: r.headers.get('content-type') || '' }))
  const a = document.createElement('a')
  a.href = url
  a.download = key.split('/').pop()
  a.click()
  setTimeout(() => URL.revokeObjectURL(url), 30000)
}
