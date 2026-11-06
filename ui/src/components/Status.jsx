// A status is never colour alone: every level has its own icon shape and a text label.
const ICONS = { good: '●', warning: '▲', serious: '◆', critical: '✕', neutral: '○' }

export function Status({ level, label }) {
  return (
    <span className={`status status-${level}`}>
      <span className="status-icon" aria-hidden="true">{ICONS[level]}</span>
      <span>{label}</span>
    </span>
  )
}
