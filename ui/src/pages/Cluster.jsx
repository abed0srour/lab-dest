import { useEffect, useRef, useState } from 'react'
import * as api from '../api.js'
import { bytes, count } from '../format.js'
import BarChart from '../components/BarChart.jsx'
import { Status } from '../components/Status.jsx'

const REFRESH_MS = 2000

function nodeStatus(n) {
  if (n.status === 'failed') return { level: 'critical', label: 'Failed' }
  if (n.last_heartbeat_s > 7) return { level: 'warning', label: `No heartbeat for ${Math.round(n.last_heartbeat_s)} s` }
  return { level: 'good', label: 'Alive' }
}

// Progress of the current repair wave: counted from the moment the queue stopped being empty.
function useRepairProgress(queue) {
  const start = useRef(null)
  const idle = useRef(true)
  if (!queue) return null
  const active = queue.pending + queue.running
  const finished = queue.done + queue.failed
  if (active > 0 && idle.current) {
    start.current = finished
    idle.current = false
  }
  if (active === 0) idle.current = true
  if (start.current === null) return null
  const done = finished - start.current
  return { done, total: done + active, active }
}

function Tile({ label, value, children }) {
  return (
    <div className="tile">
      <span className="tile-label">{label}</span>
      <span className="tile-value">{value}</span>
      {children && <span className="tile-note">{children}</span>}
    </div>
  )
}

export default function Cluster() {
  const [state, setState] = useState(null)
  const [error, setError] = useState(null)
  const [scrub, setScrub] = useState(null)

  useEffect(() => {
    let live = true
    async function tick() {
      try {
        const [status, repairs, index] = await Promise.all([
          api.clusterStatus(), api.repairStats(), api.searchStatus().catch(() => null)])
        if (live) {
          setState({ status, repairs, index, at: new Date() })
          setError(null)
        }
      } catch (e) {
        if (live) setError(e.message)
      }
    }
    tick()
    const timer = setInterval(tick, REFRESH_MS)
    return () => { live = false; clearInterval(timer) }
  }, [])

  const progress = useRepairProgress(state?.status.repair_jobs)
  if (!state) return error ? <p className="error">{error}</p> : <p className="muted">Loading cluster state…</p>

  const { status, repairs, index } = state
  const { summary, objects, chunks, repair_jobs: queue, settings } = status
  const failed = summary.failed.length
  const rows = status.nodes.map((n) => ({
    label: n.node_id.replace('storage-', ''),
    value: n.blobs_in_metadata,
    dim: n.status === 'failed',
    note: n.status === 'failed' ? ' (failed)' : '',
    detail: `${count(n.blobs_on_disk)} blobs on disk · ${bytes(n.free_bytes)} free`,
  }))

  async function runScrub() {
    try {
      const r = await api.scrubNow()
      setScrub(`Scrub started on ${r.scrub_queued_for.length} nodes: every stored blob is re-hashed.`)
    } catch (e) {
      setScrub(`Scrub failed: ${e.message}`)
    }
  }

  return (
    <div className="cluster">
      {error && <p className="error">Last refresh failed: {error}</p>}
      <div className="tiles">
        <Tile label="Storage nodes" value={`${summary.alive} of ${summary.nodes} alive`}>
          {failed ? <Status level="critical" label={`${failed} failed: ${summary.failed.map((f) => f.replace('storage-', '')).join(', ')}`} />
            : <Status level="good" label="All nodes alive" />}
        </Tile>
        <Tile label="Objects" value={count(objects.total)}>
          {objects.degraded ? <Status level="warning" label={`${count(objects.degraded)} degraded`} />
            : <Status level="good" label="All at full redundancy" />}
        </Tile>
        <Tile label="Unreadable objects" value={count(objects.lost)}>
          {objects.lost ? <Status level="critical" label="Data unavailable" /> : <Status level="good" label="None" />}
        </Tile>
        <Tile label="Repair queue" value={count(queue.pending + queue.running)}>
          <span className="muted">{count(queue.done)} done · {count(queue.failed)} failed</span>
        </Tile>
        <Tile label="Free space (all nodes)" value={bytes(summary.free_bytes)} />
        <Tile label="Search index" value={index ? count(index.indexed_objects) : '–'}>
          {index && <Status level={['green', 'yellow'].includes(index.elasticsearch) ? 'good' : 'critical'}
                            label={`Elasticsearch ${index.elasticsearch}`} />}
        </Tile>
      </div>

      <div className="cluster-grid">
        <section className="card">
          <div className="card-head">
            <div>
              <h2>Chunk distribution</h2>
              <p className="muted small">Blobs (replicas and fragments) stored on each node, from the metadata</p>
            </div>
          </div>
          <BarChart rows={rows} format={count} label="Blobs stored per storage node" />
        </section>

        <section className="card">
          <h2>Repair</h2>
          {progress && progress.active > 0 ? (
            <>
              <p><strong>{count(progress.done)}</strong> of {count(progress.total)} repair jobs done</p>
              <div className="meter large" role="progressbar" aria-valuemin={0} aria-valuemax={100}
                   aria-valuenow={Math.round((progress.done / progress.total) * 100)} aria-label="Repair progress">
                <span className="meter-fill uploading" style={{ width: `${(progress.done / progress.total) * 100}%` }} />
              </div>
            </>
          ) : progress ? (
            <Status level="good" label={`Last repair finished: ${count(progress.done)} jobs`} />
          ) : (
            <Status level="good" label="Nothing to repair" />
          )}
          <table className="compact">
            <thead>
              <tr><th>Redundancy</th><th className="num">Rebuilt</th><th className="num">Read</th><th className="num">Written</th></tr>
            </thead>
            <tbody>
              {repairs.by_redundancy.map((r) => (
                <tr key={r.redundancy}>
                  <td>{r.redundancy === 'ec' ? 'Erasure coding' : 'Replication'}</td>
                  <td className="num">{count(r.rebuilt)}</td>
                  <td className="num">{bytes(r.bytes_read)}</td>
                  <td className="num">{bytes(r.bytes_written)}</td>
                </tr>
              ))}
              {!repairs.by_redundancy.length && <tr><td colSpan={4} className="muted">No repairs yet</td></tr>}
            </tbody>
          </table>
          <div className="row">
            <button className="btn" onClick={runScrub}>Scrub now</button>
            {scrub && <span className="muted small">{scrub}</span>}
          </div>
        </section>
      </div>

      <section className="card">
        <div className="card-head">
          <h2>Storage nodes</h2>
          <span className="muted small">Updated {state.at.toLocaleTimeString('en-GB')} · refreshes every {REFRESH_MS / 1000} s</span>
        </div>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Node</th><th>Status</th><th className="num">Last heartbeat</th><th className="num">Free space</th>
                <th className="num">Blobs on disk</th><th className="num">Blobs in metadata</th>
              </tr>
            </thead>
            <tbody>
              {status.nodes.map((n) => {
                const s = nodeStatus(n)
                return (
                  <tr key={n.node_id}>
                    <td>{n.node_id}</td>
                    <td><Status level={s.level} label={s.label} /></td>
                    <td className="num">{n.last_heartbeat_s.toFixed(1)} s ago</td>
                    <td className="num">{bytes(n.free_bytes)}</td>
                    <td className="num">{count(n.blobs_on_disk)}</td>
                    <td className="num">{count(n.blobs_in_metadata)}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>

      <section className="card">
        <h2>Chunk health</h2>
        <table className="compact">
          <thead>
            <tr><th>Redundancy</th><th className="num">Healthy</th><th className="num">Degraded</th><th className="num">Lost</th></tr>
          </thead>
          <tbody>
            {Object.entries(chunks.by_redundancy).map(([red, c]) => (
              <tr key={red}>
                <td>{red === 'ec' ? `Erasure coding ${settings.ec}` : `Replication × ${settings.N}`}</td>
                <td className="num">{count(c.healthy)}</td>
                <td className="num">{c.degraded ? <Status level="warning" label={count(c.degraded)} /> : 0}</td>
                <td className="num">{c.lost ? <Status level="critical" label={count(c.lost)} /> : 0}</td>
              </tr>
            ))}
            {!Object.keys(chunks.by_redundancy).length && <tr><td colSpan={4} className="muted">No data stored yet</td></tr>}
          </tbody>
        </table>
        <p className="muted small">
          Replication N = {settings.N}, write quorum W = {settings.W}, read quorum R = {settings.R} ·
          erasure coding {settings.ec} · a node is marked failed after {settings.node_timeout_s} s without a heartbeat
        </p>
      </section>
    </div>
  )
}
