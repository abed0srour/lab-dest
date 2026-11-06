import { useState } from 'react'

// Horizontal bar chart for one series. Bars grow from one baseline, are at most 20px thick with a
// 4px rounded end, and carry their value at the tip. Hovering or focusing a row shows a tooltip;
// the same numbers are in the table below the chart, so the tooltip never hides anything.
export default function BarChart({ rows, format = String, label }) {
  const [tip, setTip] = useState(null)
  const max = Math.max(1, ...rows.map((r) => r.value))

  const show = (row, e) => {
    const box = e.currentTarget.getBoundingClientRect()
    const host = e.currentTarget.closest('.barchart').getBoundingClientRect()
    const x = e.clientX ? e.clientX - host.left : box.left - host.left + box.width / 2
    setTip({ row, x, y: box.top - host.top })
  }

  return (
    <div className="barchart" role="img" aria-label={label} onPointerLeave={() => setTip(null)}>
      {rows.map((row) => (
        <div
          key={row.label}
          className={`bar-row ${tip?.row === row ? 'hover' : ''}`}
          tabIndex={0}
          onPointerMove={(e) => show(row, e)}
          onFocus={(e) => show(row, e)}
          onBlur={() => setTip(null)}
        >
          <span className="bar-label">
            {row.label}
            {row.note && <span className="bar-note">{row.note}</span>}
          </span>
          <span className="bar-track">
            <span className={`bar ${row.dim ? 'dim' : ''}`} style={{ width: `${(row.value / max) * 100}%` }} />
            <span className="bar-value">{format(row.value)}</span>
          </span>
        </div>
      ))}
      {tip && (
        <div className="tooltip" style={{ left: tip.x, top: tip.y }} role="tooltip">
          <strong>{format(tip.row.value)}</strong>
          <span>{tip.row.label}</span>
          {tip.row.detail && <span className="muted">{tip.row.detail}</span>}
        </div>
      )}
    </div>
  )
}
