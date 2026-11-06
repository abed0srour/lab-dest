import { useEffect, useState } from 'react'
import * as api from '../api.js'
import { bytes, count, tagList, when } from '../format.js'
import { Status } from '../components/Status.jsx'

const EMPTY = { q: '', bucket: '', prefix: '', minKb: '', maxKb: '', after: '', before: '', tags: '' }
const FACETS = [['buckets', 'Bucket'], ['tags', 'Tag'], ['content_types', 'Content type']]

export default function Search() {
  const [form, setForm] = useState(EMPTY)
  const [engine, setEngine] = useState('elasticsearch')
  const [result, setResult] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const [buckets, setBuckets] = useState([])
  const [index, setIndex] = useState(null)

  const set = (field) => (e) => setForm((f) => ({ ...f, [field]: e.target.value }))

  async function run(values = form, eng = engine) {
    setLoading(true)
    const tags = values.tags.split(',').map((t) => t.trim()).filter(Boolean)
    const params = {
      engine: eng, bucket: values.bucket, prefix: values.prefix, tag: tags, limit: 200,
      min_size: values.minKb === '' ? '' : Math.round(values.minKb * 1024),
      max_size: values.maxKb === '' ? '' : Math.round(values.maxKb * 1024),
      after: values.after, before: values.before,
      q: eng === 'elasticsearch' ? values.q.trim() : '',
    }
    const t0 = performance.now()
    try {
      const r = await api.search(params)
      setResult({ ...r, engine: eng, roundTripMs: Math.round(performance.now() - t0) })
      setError(null)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
    api.searchStatus().then(setIndex).catch(() => setIndex(null))
  }

  useEffect(() => {
    api.listBuckets().then(setBuckets).catch(() => {})
    run()
  }, [])

  function refine(kind, value) {
    const next = kind === 'buckets' ? { ...form, bucket: value }
      : kind === 'tags' ? { ...form, tags: [...new Set([...form.tags.split(',').map((t) => t.trim()).filter(Boolean), value])].join(', ') }
        : form
    setForm(next)
    run(next)
  }

  function switchEngine(eng) {
    setEngine(eng)
    run(form, eng)
  }

  return (
    <div className="search-page">
      <form className="card search-form" onSubmit={(e) => { e.preventDefault(); run() }}>
        <div className="search-main">
          <input className="search-box" value={form.q} onChange={set('q')} disabled={engine !== 'elasticsearch'}
                 placeholder={engine === 'elasticsearch' ? 'Search keys and tags, e.g. disk, user-0001, type=note (typos are fine)' : 'Free text needs Elasticsearch: use the filters below'}
                 aria-label="Search text" />
          <div className="segmented" role="radiogroup" aria-label="Search engine">
            {[['elasticsearch', 'Elasticsearch'], ['postgres', 'PostgreSQL']].map(([id, label]) => (
              <button type="button" key={id} role="radio" aria-checked={engine === id}
                      className={engine === id ? 'on' : ''} onClick={() => switchEngine(id)}>{label}</button>
            ))}
          </div>
          <button className="btn primary" type="submit">Search</button>
        </div>
        <div className="search-filters">
          <label>Bucket
            <select value={form.bucket} onChange={set('bucket')}>
              <option value="">All buckets</option>
              {buckets.map((b) => <option key={b.name} value={b.name}>{b.name}</option>)}
            </select>
          </label>
          <label>Key prefix<input value={form.prefix} onChange={set('prefix')} placeholder="notes/" /></label>
          <label>Min size (KB)<input type="number" min="0" value={form.minKb} onChange={set('minKb')} /></label>
          <label>Max size (KB)<input type="number" min="0" value={form.maxKb} onChange={set('maxKb')} /></label>
          <label>Uploaded from<input type="date" value={form.after} onChange={set('after')} /></label>
          <label>Uploaded before<input type="date" value={form.before} onChange={set('before')} /></label>
          <label className="wide">Tags (all must match)<input value={form.tags} onChange={set('tags')} placeholder="type=note, project=lab" /></label>
          <button type="button" className="btn" onClick={() => { setForm(EMPTY); run(EMPTY) }}>Clear</button>
        </div>
      </form>

      <div className="search-status">
        {index && (
          <Status level={index.elasticsearch === 'green' || index.elasticsearch === 'yellow' ? 'good' : 'critical'}
                  label={`Elasticsearch ${index.elasticsearch} · ${count(index.indexed_objects)} objects indexed`
                    + (index.waiting_to_index ? ` · ${count(index.waiting_to_index)} waiting` : '')} />
        )}
        {result && (
          <span className="muted">
            {count(result.count)} result(s) from {result.engine === 'elasticsearch' ? 'Elasticsearch' : 'PostgreSQL'}
            {result.took_ms !== undefined && ` · search took ${result.took_ms} ms`} · round trip {result.roundTripMs} ms
          </span>
        )}
      </div>
      {error && <p className="error">{error}</p>}

      <div className="search-body">
        {result?.facets && (
          <aside className="card facets" aria-label="Refine results">
            {FACETS.map(([name, title]) => result.facets[name]?.length > 0 && (
              <div key={name} className="facet">
                <h3>{title}</h3>
                <ul>
                  {result.facets[name].map((f) => (
                    <li key={f.value}>
                      {name === 'content_types' ? <span className="facet-item static">{f.value}</span> : (
                        <button className="facet-item" onClick={() => refine(name, f.value)}>{f.value}</button>
                      )}
                      <span className="facet-count">{count(f.count)}</span>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </aside>
        )}
        <div className={`card results table-wrap ${loading ? 'refetching' : ''}`}>
          <table>
            <thead>
              <tr>
                <th>Bucket</th>
                <th>Key</th>
                <th className="num">Size</th>
                <th>Tags</th>
                <th>Uploaded</th>
                {result?.engine === 'elasticsearch' && form.q && <th className="num">Score</th>}
              </tr>
            </thead>
            <tbody>
              {(result?.results || []).map((o) => (
                <tr key={`${o.bucket}/${o.key}`}>
                  <td>{o.bucket}</td>
                  <td className="key">{o.key}</td>
                  <td className="num">{bytes(o.size)}</td>
                  <td>{tagList(o.tags).map((t) => <span className="chip" key={t}>{t}</span>)}</td>
                  <td className="nowrap">{when(o.created_at)}</td>
                  {result.engine === 'elasticsearch' && form.q && <td className="num">{o.score?.toFixed(2)}</td>}
                </tr>
              ))}
            </tbody>
          </table>
          {result && !result.results.length && <p className="muted pad">Nothing matches these filters.</p>}
        </div>
      </div>
    </div>
  )
}
