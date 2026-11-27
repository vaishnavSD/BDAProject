import { X, AlertTriangle, AlertOctagon, Info } from 'lucide-react';
import { resolveAlert } from '../services/api';

const SEVERITY_STYLE = {
  CRITICAL: { bg: '#450a0a', border: '#dc2626', icon: <AlertOctagon size={16} color="#f87171" />, tag: '#ef4444' },
  WARNING:  { bg: '#451a03', border: '#d97706', icon: <AlertTriangle size={16} color="#fbbf24" />, tag: '#f59e0b' },
  INFO:     { bg: '#0c1a2e', border: '#3b82f6', icon: <Info size={16} color="#60a5fa" />, tag: '#3b82f6' },
};

export default function AlertsBanner({ alerts = [], onDismiss }) {
  if (!alerts.length) return null;

  const handleResolve = async (alertId) => {
    try { await resolveAlert(alertId); } catch (_) {}
    onDismiss(alertId);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 8, marginBottom: 20 }}>
      {alerts.slice(0, 5).map(alert => {
        const style = SEVERITY_STYLE[alert.severity] || SEVERITY_STYLE.INFO;
        return (
          <div key={alert.alert_id} style={{
            background: style.bg, border: `1px solid ${style.border}`,
            borderRadius: 8, padding: '10px 14px',
            display: 'flex', alignItems: 'center', gap: 10,
          }}>
            {style.icon}
            <span style={{
              background: style.tag, color: '#fff',
              borderRadius: 4, fontSize: 10, fontWeight: 700,
              padding: '2px 6px', flexShrink: 0,
            }}>{alert.alert_type}</span>
            <span style={{ fontSize: 13, color: '#e2e8f0', flex: 1, lineHeight: 1.4 }}>
              {alert.message}
            </span>
            {alert.product_id && (
              <span style={{ fontSize: 11, color: '#94a3b8', flexShrink: 0 }}>
                {alert.product_id} · {alert.store_id}
              </span>
            )}
            <button onClick={() => handleResolve(alert.alert_id)} style={{
              background: 'none', border: 'none', cursor: 'pointer',
              color: '#64748b', flexShrink: 0, padding: 2,
              display: 'flex', alignItems: 'center',
            }}>
              <X size={14} />
            </button>
          </div>
        );
      })}
      {alerts.length > 5 && (
        <div style={{ fontSize: 12, color: '#64748b', textAlign: 'center' }}>
          +{alerts.length - 5} more alerts
        </div>
      )}
    </div>
  );
}
