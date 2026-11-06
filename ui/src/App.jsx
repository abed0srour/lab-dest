import { useEffect, useState } from 'react'
import { getKey, health, listBuckets, setKey } from './api.js'
import Files from './pages/Files.jsx'
import Search from './pages/Search.jsx'
import Cluster from './pages/Cluster.jsx'
import { Status } from './components/Status.jsx'

const PAGES = [
  ['files', 'Files'],
  ['search', 'Search'],
  ['cluster', 'Cluster'],
]

function usePage() {
  const read = () => location.hash.replace(/^#\/?/, '') || 'files'
  const [page, setPage] = useState(read)
  useEffect(() => {
    const onChange = () => setPage(read())
    window.addEventListener('hashchange', onChange)
    return () => window.removeEventListener('hashchange', onChange)
  }, [])
  return page
}

export default function App() {
  const page = usePage()
  const [apiKey, setApiKey] = useState(getKey())
  const [draft, setDraft] = useState(apiKey)
  const [conn, setConn] = useState({ state: 'checking' })

  useEffect(() => {
    let live = true
    Promise.all([health(), listBuckets()])
      .then(([h]) => live && setConn({ state: 'ok', gateway: h.gateway }))
      .catch((e) => live && setConn({ state: 'error', message: e.status === 401 ? 'API key rejected' : e.message }))
    return () => { live = false }
  }, [apiKey])

  function applyKey(e) {
    e.preventDefault()
    setKey(draft.trim())
    setApiKey(draft.trim())
  }

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden="true" />
          <span>lab-dest</span>
          <span className="brand-sub">distributed object storage</span>
        </div>
        <nav aria-label="Pages">
          {PAGES.map(([id, label]) => (
            <a key={id} href={`#/${id}`} className={page === id ? 'active' : ''} aria-current={page === id ? 'page' : undefined}>
              {label}
            </a>
          ))}
        </nav>
        <form className="keyform" onSubmit={applyKey}>
          <label htmlFor="apikey">API key</label>
          <input id="apikey" value={draft} onChange={(e) => setDraft(e.target.value)} spellCheck={false} autoComplete="off" />
          <button type="submit" className="btn">Use key</button>
          <span className="conn" role="status">
            {conn.state === 'ok' && <Status level="good" label={`Connected to ${conn.gateway}`} />}
            {conn.state === 'error' && <Status level="critical" label={conn.message} />}
            {conn.state === 'checking' && <span className="muted">Connecting…</span>}
          </span>
        </form>
      </header>
      <main key={apiKey}>
        {page === 'search' ? <Search /> : page === 'cluster' ? <Cluster /> : <Files />}
      </main>
    </div>
  )
}
