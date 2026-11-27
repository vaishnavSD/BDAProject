export default function SupplierView({ data = [], loading = false }) {
  return (
    <div style={{ background: '#1e293b', borderRadius: 10, border: '1px solid #334155', overflow: 'hidden' }}>
      <div style={{ padding: '14px 16px', borderBottom: '1px solid #334155' }}>
        <span style={{ fontWeight: 600, fontSize: 14, color: '#f1f5f9' }}>Supplier Performance</span>
      </div>
      {loading ? (
        <div style={{ padding: 32, textAlign: 'center', color: '#64748b' }}>Loading…</div>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                {['Supplier', 'Period', 'Orders', 'On-Time %', 'Fill Rate %', 'Avg Lead (days)', 'Freight'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontSize: 12, color: '#94a3b8', fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.length === 0 && (
                <tr><td colSpan={7} style={{ padding: 24, textAlign: 'center', color: '#64748b' }}>No data</td></tr>
              )}
              {data.map((row, i) => {
                const rate = parseFloat(row.on_time_rate);
                const rateColor = rate < 50 ? '#ef4444' : rate < 70 ? '#fbbf24' : '#22c55e';
                return (
                  <tr key={i} style={{ borderBottom: '1px solid #1e293b' }}>
                    <td style={{ padding: '8px 14px', color: '#38bdf8', fontWeight: 500 }}>{row.supplier_id}</td>
                    <td style={{ padding: '8px 14px', color: '#64748b', fontSize: 12 }}>
                      {row.report_period_start} → {row.report_period_end}
                    </td>
                    <td style={{ padding: '8px 14px', textAlign: 'center' }}>{row.total_orders}</td>
                    <td style={{ padding: '8px 14px', textAlign: 'center', color: rateColor, fontWeight: 700 }}>
                      {row.on_time_rate ?? '—'}%
                    </td>
                    <td style={{ padding: '8px 14px', textAlign: 'center', color: '#94a3b8' }}>
                      {row.fill_rate ?? '—'}%
                    </td>
                    <td style={{ padding: '8px 14px', textAlign: 'center', color: '#94a3b8' }}>
                      {row.avg_lead_time_days ?? '—'}
                    </td>
                    <td style={{ padding: '8px 14px', textAlign: 'right', color: '#94a3b8' }}>
                      ₹{row.total_freight_cost ? Number(row.total_freight_cost).toLocaleString('en-IN') : '—'}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
