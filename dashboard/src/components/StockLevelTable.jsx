import { useState } from 'react';
import { ChevronUp, ChevronDown } from 'lucide-react';

function SortIcon({ col, sortCol, sortDir }) {
  if (sortCol !== col) return <span style={{ color: '#334155' }}>⇅</span>;
  return sortDir === 'asc' ? <ChevronUp size={12} /> : <ChevronDown size={12} />;
}

export default function StockLevelTable({ data = [], loading = false }) {
  const [sortCol, setSortCol] = useState('closing_stock');
  const [sortDir, setSortDir] = useState('asc');
  const [filter, setFilter]   = useState('');

  const handleSort = (col) => {
    if (sortCol === col) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortCol(col); setSortDir('asc'); }
  };

  const filtered = data.filter(r =>
    !filter ||
    r.product_id.toLowerCase().includes(filter.toLowerCase()) ||
    r.store_id.toLowerCase().includes(filter.toLowerCase())
  );

  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortCol], bv = b[sortCol];
    const cmp = av < bv ? -1 : av > bv ? 1 : 0;
    return sortDir === 'asc' ? cmp : -cmp;
  });

  const TH = ({ col, label }) => (
    <th onClick={() => handleSort(col)} style={{
      padding: '10px 14px', textAlign: 'left', fontSize: 12,
      color: '#94a3b8', fontWeight: 600, cursor: 'pointer',
      whiteSpace: 'nowrap', userSelect: 'none',
      borderBottom: '1px solid #334155',
    }}>
      {label} <SortIcon col={col} sortCol={sortCol} sortDir={sortDir} />
    </th>
  );

  return (
    <div style={{ background: '#1e293b', borderRadius: 10, border: '1px solid #334155', overflow: 'hidden' }}>
      {/* Header bar */}
      <div style={{ padding: '14px 16px', borderBottom: '1px solid #334155', display: 'flex', alignItems: 'center', gap: 12 }}>
        <span style={{ fontWeight: 600, fontSize: 14, color: '#f1f5f9', flex: 1 }}>Stock Levels</span>
        <input
          value={filter}
          onChange={e => setFilter(e.target.value)}
          placeholder="Filter product / store…"
          style={{
            background: '#0f172a', border: '1px solid #334155', borderRadius: 6,
            padding: '5px 10px', fontSize: 12, color: '#e2e8f0', width: 220,
            outline: 'none',
          }}
        />
      </div>

      {loading ? (
        <div style={{ padding: 32, textAlign: 'center', color: '#64748b' }}>Loading…</div>
      ) : (
        <div style={{ overflowX: 'auto', maxHeight: 420, overflowY: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead style={{ position: 'sticky', top: 0, background: '#1e293b' }}>
              <tr>
                <TH col="product_id"    label="Product" />
                <TH col="store_id"      label="Store" />
                <TH col="snapshot_date" label="Date" />
                <TH col="opening_stock" label="Opening" />
                <TH col="total_inbound" label="Inbound" />
                <TH col="total_outbound" label="Outbound" />
                <TH col="closing_stock" label="Closing" />
                <TH col="reorder_point" label="Reorder Pt" />
                <TH col="is_below_reorder" label="Status" />
              </tr>
            </thead>
            <tbody>
              {sorted.length === 0 && (
                <tr><td colSpan={9} style={{ padding: 24, textAlign: 'center', color: '#64748b' }}>No data</td></tr>
              )}
              {sorted.map((row, i) => (
                <tr key={i} style={{
                  background: row.is_below_reorder ? 'rgba(220,38,38,0.08)' : 'transparent',
                  borderBottom: '1px solid #1e293b',
                }}>
                  <td style={{ padding: '8px 14px', color: '#38bdf8', fontWeight: 500 }}>{row.product_id}</td>
                  <td style={{ padding: '8px 14px', color: '#94a3b8' }}>{row.store_id}</td>
                  <td style={{ padding: '8px 14px', color: '#94a3b8' }}>{row.snapshot_date}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right' }}>{row.opening_stock}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: '#22c55e' }}>+{row.total_inbound}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: '#f87171' }}>-{row.total_outbound}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', fontWeight: 700,
                    color: row.closing_stock === 0 ? '#ef4444' : row.is_below_reorder ? '#fbbf24' : '#f1f5f9',
                  }}>{row.closing_stock}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: '#64748b' }}>{row.reorder_point}</td>
                  <td style={{ padding: '8px 14px' }}>
                    {row.is_below_reorder ? (
                      <span style={{ background: '#7f1d1d', color: '#fca5a5', borderRadius: 4, padding: '2px 8px', fontSize: 11, fontWeight: 600 }}>
                        {row.closing_stock === 0 ? 'STOCKOUT' : 'LOW'}
                      </span>
                    ) : (
                      <span style={{ background: '#14532d', color: '#86efac', borderRadius: 4, padding: '2px 8px', fontSize: 11, fontWeight: 600 }}>OK</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
