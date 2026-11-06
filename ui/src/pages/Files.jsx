import { Fragment, useEffect, useRef, useState } from 'react'
import * as api from '../api.js'
import { bytes, count, redundancyLabel, tagList, when } from '../format.js'

const BUCKET_NAME = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/

export default function Files() {
  const [buckets, setBuckets] = useState([])
  const [current, setCurrent] = useState(null)
  const [error, setError] = useState(null)

  async function reload(select) {
    try {
      const list = await api.listBuckets()
      setBuckets(list)
      setError(null)
      setCurrent((c) => select || c || list[0]?.name || null)
    } catch (e) {
      setError(e.message)
    }
  }
  useEffect(() => { reload() }, [])

  const bucket = buckets.find((b) => b.name === current)
  return (
    <div className="files">
      <aside className="card bucket-list">
        <h2>Buckets</h2>
        {error && <p className="error">{error}</p>}
        <ul>
          {buckets.map((b) => (
            <li key={b.name}>
              <button className={`bucket ${b.name === current ? 'active' : ''}`} onClick={() => setCurrent(b.name)}>
                <span className="bucket-name">{b.name}</span>
                <span className="bucket-meta">
                  {redundancyLabel(b.redundancy)} · {count(b.objects)} objects
                </span>
              </button>
            </li>
          ))}
        </ul>
        {!buckets.length && !error && <p className="muted">No buckets yet: create one below.</p>}
        <NewBucket onCreated={(name) => reload(name)} />
      </aside>
      <section className="file-main">
        {bucket ? <Bucket key={bucket.name} bucket={bucket} onChange={() => reload()} /> : (
          <div className="card empty">Choose or create a bucket to upload files.</div>
        )}
      </section>
    </div>
  )
}

function NewBucket({ onCreated }) {
  const [name, setName] = useState('')
  const [redundancy, setRedundancy] = useState('replication')
  const [message, setMessage] = useState(null)

  async function submit(e) {
    e.preventDefault()
    if (!BUCKET_NAME.test(name)) return setMessage('Use 3–63 lowercase letters, digits, dots or dashes.')
    try {
      await api.createBucket(name, redundancy)
      setName('')
      setMessage(null)
      onCreated(name)
    } catch (err) {
      setMessage(err.message)
    }
  }

  return (
    <form className="stack new-bucket" onSubmit={submit}>
      <h3>New bucket</h3>
      <label>
        Name
        <input value={name} onChange={(e) => setName(e.target.value.toLowerCase())} placeholder="my-photos" />
      </label>
      <label>
        Redundancy
        <select value={redundancy} onChange={(e) => setRedundancy(e.target.value)}>
          <option value="replication">Replication: 3 copies (3× space)</option>
          <option value="ec">Erasure coding: 4 + 2 (1.5× space)</option>
        </select>
      </label>
      <button className="btn primary" type="submit">Create bucket</button>
      {message && <p className="error small">{message}</p>}
    </form>
  )
}

function Bucket({ bucket, onChange }) {
  const [prefix, setPrefix] = useState('')
  const [objects, setObjects] = useState([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [open, setOpen] = useState(null)
  const [downloads, setDownloads] = useState({})

  async function load() {
    setLoading(true)
    try {
      setObjects(await api.listObjects(bucket.name, prefix))
      setError(null)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }
  useEffect(() => {
    const t = setTimeout(load, 200)
    return () => clearTimeout(t)
  }, [prefix])

  async function download(o) {
    setDownloads((d) => ({ ...d, [o.key]: 0 }))
    try {
      await api.downloadObject(bucket.name, o.key, o.size, (got, total) =>
        setDownloads((d) => ({ ...d, [o.key]: total ? Math.floor((got / total) * 100) : 0 })))
    } catch (e) {
      alert(`Download failed: ${e.message}`)
    } finally {
      setDownloads(({ [o.key]: _, ...rest }) => rest)
    }
  }

  async function remove(o) {
    if (!confirm(`Delete ${o.key} and all its versions?`)) return
    try {
      await api.deleteObject(bucket.name, o.key)
      await load()
      onChange()
    } catch (e) {
      alert(`Delete failed: ${e.message}`)
    }
  }

  return (
    <>
      <Uploader bucket={bucket} onUploaded={() => { load(); onChange() }} />
      <div className="card">
        <div className="card-head">
          <div>
            <h2>{bucket.name}</h2>
            <p className="muted small">{redundancyLabel(bucket.redundancy)}</p>
          </div>
          <input className="filter" value={prefix} onChange={(e) => setPrefix(e.target.value)}
                 placeholder="Filter by key prefix, e.g. notes/" aria-label="Key prefix" />
        </div>
        {error && <p className="error">{error}</p>}
        <div className={`table-wrap ${loading ? 'refetching' : ''}`}>
          <table>
            <thead>
              <tr>
                <th>Key</th>
                <th className="num">Size</th>
                <th className="num">Version</th>
                <th>Tags</th>
                <th>Uploaded</th>
                <th><span className="sr-only">Actions</span></th>
              </tr>
            </thead>
            <tbody>
              {objects.map((o) => (
                <Fragment key={o.key}>
                  <tr>
                    <td className="key">{o.key}</td>
                    <td className="num">{bytes(o.size)}</td>
                    <td className="num">v{o.version}</td>
                    <td>{tagList(o.tags).map((t) => <span className="chip" key={t}>{t}</span>)}</td>
                    <td className="nowrap">{when(o.created_at)}</td>
                    <td className="actions">
                      <button className="btn small" onClick={() => download(o)} disabled={o.key in downloads}>
                        {o.key in downloads ? `${downloads[o.key]}%` : 'Download'}
                      </button>
                      <button className="btn small" onClick={() => setOpen(open === o.key ? null : o.key)}
                              aria-expanded={open === o.key}>Details</button>
                      <button className="btn small danger" onClick={() => remove(o)}>Delete</button>
                    </td>
                  </tr>
                  {open === o.key && (
                    <tr className="details-row">
                      <td colSpan={6}><Details bucket={bucket.name} objectKey={o.key} /></td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </tbody>
          </table>
        </div>
        {!objects.length && !loading && (
          <p className="muted pad">{prefix ? `No objects start with “${prefix}”.` : 'This bucket is empty: upload a file above.'}</p>
        )}
        <p className="muted small pad">{count(objects.length)} object(s)</p>
      </div>
    </>
  )
}

function Details({ bucket, objectKey }) {
  const [meta, setMeta] = useState(null)
  const [error, setError] = useState(null)
  useEffect(() => { api.objectMetadata(bucket, objectKey).then(setMeta, (e) => setError(e.message)) }, [bucket, objectKey])
  if (error) return <p className="error">{error}</p>
  if (!meta) return <p className="muted">Loading metadata…</p>
  const ec = meta.redundancy === 'ec'
  return (
    <div className="details">
      <dl className="facts">
        <div><dt>SHA-256 (ETag)</dt><dd className="mono">{meta.sha256}</dd></div>
        <div><dt>Content type</dt><dd>{meta.content_type || '–'}</dd></div>
        <div><dt>Redundancy</dt><dd>{redundancyLabel(meta.redundancy)}</dd></div>
        <div><dt>Chunking</dt><dd>{meta.chunking === 'cdc' ? 'FastCDC' : 'Fixed 4 MB'}</dd></div>
        <div><dt>Chunks</dt><dd>{count(meta.chunks.length)}</dd></div>
      </dl>
      <table className="chunks">
        <thead>
          <tr>
            <th className="num">#</th>
            <th>Chunk SHA-256</th>
            <th className="num">Size</th>
            <th className="num">References</th>
            <th>{ec ? 'Fragment : node' : 'Copies on'}</th>
          </tr>
        </thead>
        <tbody>
          {meta.chunks.slice(0, 40).map((c) => (
            <tr key={c.seq}>
              <td className="num">{c.seq}</td>
              <td className="mono">{c.hash.slice(0, 16)}…</td>
              <td className="num">{bytes(c.size)}</td>
              <td className="num">{c.refcount}</td>
              <td>{c.stored_on.map((s) => <span className="chip node" key={s}>{s.replace('storage-', '')}</span>)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {meta.chunks.length > 40 && <p className="muted small">… and {count(meta.chunks.length - 40)} more chunks.</p>}
    </div>
  )
}

function Uploader({ bucket, onUploaded }) {
  const [files, setFiles] = useState([])
  const [folder, setFolder] = useState('')
  const [tags, setTags] = useState('')
  const [chunking, setChunking] = useState('fixed')
  const [jobs, setJobs] = useState([])
  const [dragging, setDragging] = useState(false)
  const picker = useRef(null)

  const update = (name, patch) => setJobs((all) => {
    const i = all.findIndex((j) => j.name === name)
    if (i < 0) return [{ name, ...patch }, ...all].slice(0, 12)
    const next = [...all]
    next[i] = { ...next[i], ...patch }
    return next
  })

  async function start() {
    const list = files
    setFiles([])
    const base = folder && !folder.endsWith('/') ? `${folder}/` : folder
    for (const file of list) { // one at a time, so each progress bar is meaningful
      const key = base + file.name
      const t0 = performance.now()
      update(key, { loaded: 0, total: file.size, state: 'uploading', message: null })
      try {
        const r = await api.uploadFile(bucket.name, key, file, { tags, chunking }, (loaded, total) =>
          update(key, { loaded, total, rate: loaded / ((performance.now() - t0) / 1000) }))
        update(key, {
          state: 'done', loaded: file.size,
          message: `version ${r.version} · ${r.chunks} chunks (${r.new_chunks} new, ${r.deduplicated_chunks} already stored)`,
        })
      } catch (e) {
        update(key, { state: 'error', message: e.message })
      }
    }
    onUploaded()
  }

  function drop(e) {
    e.preventDefault()
    setDragging(false)
    setFiles([...e.dataTransfer.files])
  }

  return (
    <div className="card uploader">
      <h2>Upload to {bucket.name}</h2>
      <input ref={picker} type="file" multiple hidden onChange={(e) => setFiles([...e.target.files])} />
      <div
        className={`dropzone ${dragging ? 'over' : ''}`}
        onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
        onDragLeave={() => setDragging(false)}
        onDrop={drop}
        onClick={() => picker.current.click()}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => (e.key === 'Enter' || e.key === ' ') && picker.current.click()}
      >
        {files.length
          ? <span><strong>{files.length} file(s) selected</strong> · {bytes(files.reduce((s, f) => s + f.size, 0))}</span>
          : <span>Drop files here or <u>choose files</u></span>}
      </div>
      <div className="upload-options">
        <label>Folder (optional)<input value={folder} onChange={(e) => setFolder(e.target.value)} placeholder="photos/2026" /></label>
        <label>Tags<input value={tags} onChange={(e) => setTags(e.target.value)} placeholder="type=photo,project=lab" /></label>
        <label>Chunking
          <select value={chunking} onChange={(e) => setChunking(e.target.value)}>
            <option value="fixed">Fixed 4 MB</option>
            <option value="cdc">FastCDC (better dedup)</option>
          </select>
        </label>
        <button className="btn primary" onClick={start} disabled={!files.length}>Upload</button>
      </div>
      {jobs.length > 0 && (
        <ul className="jobs">
          {jobs.map((j) => (
            <li key={j.name}>
              <div className="job-line">
                <span className="key">{j.name}</span>
                <span className="muted small">
                  {j.state === 'uploading' && `${bytes(j.loaded)} of ${bytes(j.total)}${j.rate ? ` · ${bytes(j.rate)}/s` : ''}`}
                  {j.state === 'done' && j.message}
                </span>
              </div>
              <div className="meter" aria-label={`${j.name} upload`} role="progressbar"
                   aria-valuemin={0} aria-valuemax={100} aria-valuenow={j.total ? Math.round((j.loaded / j.total) * 100) : 0}>
                <span className={`meter-fill ${j.state}`} style={{ width: `${j.total ? (j.loaded / j.total) * 100 : 100}%` }} />
              </div>
              {j.state === 'error' && <p className="error small">{j.message}</p>}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}
