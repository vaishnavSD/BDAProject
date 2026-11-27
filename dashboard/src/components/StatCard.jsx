export default function StatCard({ label, value, icon, color = '#38bdf8', sub }) {
  return (
    <div style={{
      background: '#1e293b', borderRadius: 10,
      border: `1px solid #334155`,
      padding: '20px 24px',
      display: 'flex', alignItems: 'flex-start', gap: 16,
    }}>
      <div style={{
        width: 44, height: 44, borderRadius: 10,
        background: `${color}20`,
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        fontSize: 22, flexShrink: 0,
      }}>{icon}</div>
      <div>
        <div style={{ fontSize: 26, fontWeight: 700, color: '#f1f5f9', lineHeight: 1 }}>
          {value ?? '—'}
        </div>
        <div style={{ fontSize: 13, color: '#64748b', marginTop: 4 }}>{label}</div>
        {sub && <div style={{ fontSize: 12, color: color, marginTop: 2 }}>{sub}</div>}
      </div>
    </div>
  );
}
