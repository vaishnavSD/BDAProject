import { useState } from 'react';
import { useAlerts } from '../hooks/useInventory';
import { resolveAlert } from '../services/api';
import { useWebSocket } from '../hooks/useWebSocket';
import AlertsBanner from '../components/AlertsBanner';

const SEV_COLOR = { CRITICAL: '#ef4444', WARNING: '#f59e0b', INFO: '#3b82f6' };

export default function AlertsPage() {
  const [severity, setSeverity]         = useState('');
  const [unresolvedOnly, setUnresolved] = useState(true);
  const [page, setPage]                 = useState(1);
  const { data, loading, reload }       = useAlerts({ severity: severity || undefined, unresolved_only: unresolvedOnly, page, page_size: 20 });
  const { alerts: liveAlerts, dismiss } = useWebSocket();

  const handleResolve = async (id) => {
    await resolveAlert(id);
    reload();
  };

  return (
    <div>
      <h2 style={{ fontSize: 20, fontWeight: 700, marginBottom: 20, color: '#f1f5f9' }}>Alerts</h2>

      {/* Live stream banner */}
      {liveAlerts.length > 0 && (
        <div style={{ marginBottom: 16 }}>
          <div style={{ fontSize: 12, color: '#38bdf8', fontWeight: 600, marginBottom: 8 }}>
            🔴 LIVE — {liveAlerts.length} new alert(s)
          </div>
          <AlertsBanner alerts={liveAlerts} onDismiss={dismiss} />
        </div>
      )}

      {/* Filters */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 20, alignItems: 'center', flexWrap: 'wrap' }}>
        <select value={severity} onChange={e => { setSeverity(e.target.value); setPage(1); }} style={selectStyle}>
          <option value="">All Severities</option>
          <option value="CRITICAL">Critical</option>
          <option value="WARNING">Warning</option>
          <option value="INFO">Info</option>
        </select>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, color: '#94a3b8', cursor: 'pointer' }}>
          <input type="checkbox" checked={unresolvedOnly} onChange={e => { setUnresolved(e.target.checked); setPage(1); }} />
          Unresolved only
        </label>
        <span style={{ marginLeft: 'auto', fontSize: 13, color: '#64748b' }}>
          {data.total} alerts total
        </span>
      </div>

      {/* Table */}
      <div style={{ background: '#1e293b', borderRadius: 10, border: '1px solid #334155', overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
          <thead>
            <tr style={{ borderBottom: '1px solid #334155' }}>
              {['#', 'Type', 'Severity', 'Source', 'Product', 'Store', 'Message', 'Time', 'Action'].map(h => (
                <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontSize: 12, color: '#94a3b8', fontWeight: 600 }}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading && (
              <tr><td colSpan={9} style={{ padding: 32, textAlign: 'center', color: '#64748b' }}>Loading…</td></tr>
            )}
            {!loading && data.items.length === 0 && (
              <tr><td colSpan={9} style={{ padding: 32, textAlign: 'center', color: '#64748b' }}>No alerts found</td></tr>
            )}
            {data.items.map(a => (
              <tr key={a.alert_id} style={{ borderBottom: '1px solid #0f172a' }}>
                <td style={{ padding: '8px 14px', color: '#475569', fontSize: 11 }}>{a.alert_id}</td>
                <td style={{ padding: '8px 14px' }}>
                  <span style={{ background: '#1e3a5f', color: '#7dd3fc', borderRadius: 4, padding: '2px 7px', fontSize: 11, fontWeight: 600 }}>
                    {a.alert_type}
                  </span>
                </td>
                <td style={{ padding: '8px 14px' }}>
                  <span style={{ color: SEV_COLOR[a.severity] || '#94a3b8', fontWeight: 700, fontSize: 12 }}>{a.severity}</span>
                </td>
                <td style={{ padding: '8px 14px', color: '#64748b', fontSize: 11 }}>{a.source}</td>
                <td style={{ padding: '8px 14px', color: '#38bdf8', fontSize: 12 }}>{a.product_id ?? '—'}</td>
                <td style={{ padding: '8px 14px', color: '#94a3b8', fontSize: 12 }}>{a.store_id ?? '—'}</td>
                <td style={{ padding: '8px 14px', color: '#cbd5e1', maxWidth: 320 }}>
                  <span title={a.message}>{a.message.length > 80 ? a.message.slice(0, 80) + '…' : a.message}</span>
                </td>
                <td style={{ padding: '8px 14px', color: '#64748b', fontSize: 11, whiteSpace: 'nowrap' }}>
                  {new Date(a.created_at).toLocaleString()}
                </td>
                <td style={{ padding: '8px 14px' }}>
                  {!a.is_resolved && (
                    <button onClick={() => handleResolve(a.alert_id)} style={{
                      background: '#1e3a5f', border: '1px solid #3b82f6', color: '#60a5fa',
                      borderRadius: 5, padding: '3px 10px', cursor: 'pointer', fontSize: 11,
                    }}>Resolve</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginTop: 16 }}>
        <button onClick={() => setPage(p => Math.max(1, p - 1))} disabled={page === 1} style={btnStyle}>← Prev</button>
        <span style={{ fontSize: 13, color: '#94a3b8', padding: '6px 0' }}>Page {page} of {Math.ceil(data.total / 20) || 1}</span>
        <button onClick={() => setPage(p => p + 1)} disabled={page >= Math.ceil(data.total / 20)} style={btnStyle}>Next →</button>
      </div>
    </div>
  );
}

const selectStyle = {
  background: '#0f172a', border: '1px solid #334155', borderRadius: 6,
  color: '#e2e8f0', padding: '6px 10px', fontSize: 13, outline: 'none', cursor: 'pointer',
};
const btnStyle = {
  background: '#1e293b', border: '1px solid #334155', color: '#94a3b8',
  borderRadius: 6, padding: '6px 14px', cursor: 'pointer', fontSize: 13,
};
