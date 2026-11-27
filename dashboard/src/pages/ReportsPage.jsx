import { useState, useEffect } from 'react';
import ReportSummary from '../components/ReportSummary';
import SupplierView from '../components/SupplierView';
import { fetchOverstockReport, fetchInventoryTurnover, fetchSupplierPerformance } from '../services/api';

export default function ReportsPage() {
  const [tab, setTab]           = useState('overstock');
  const [overstock, setOs]      = useState([]);
  const [turnover, setTov]      = useState([]);
  const [suppliers, setSup]     = useState([]);
  const [loading, setLoading]   = useState(false);

  useEffect(() => {
    setLoading(true);
    Promise.all([
      fetchOverstockReport({ limit: 50 }),
      fetchInventoryTurnover({ limit: 50 }),
      fetchSupplierPerformance({ limit: 50 }),
    ]).then(([os, tov, sup]) => {
      setOs(os.data);
      setTov(tov.data);
      setSup(sup.data);
    }).finally(() => setLoading(false));
  }, []);

  const TABS = [
    { key: 'overstock',  label: 'Overstock Report' },
    { key: 'turnover',   label: 'Inventory Turnover' },
    { key: 'suppliers',  label: 'Supplier Performance' },
  ];

  const overstockChart = ['HIGH','MEDIUM','LOW'].map(s => ({
    severity: s, count: overstock.filter(r => r.overstock_severity === s).length,
  }));

  // Top 10 by turnover ratio
  const turnoverChart = [...turnover]
    .sort((a, b) => (b.turnover_ratio || 0) - (a.turnover_ratio || 0))
    .slice(0, 10)
    .map(r => ({ id: r.product_id, ratio: parseFloat(r.turnover_ratio || 0).toFixed(2) }));

  return (
    <div>
      <h2 style={{ fontSize: 20, fontWeight: 700, marginBottom: 20, color: '#f1f5f9' }}>Batch Reports</h2>

      {/* Tab bar */}
      <div style={{ display: 'flex', gap: 4, marginBottom: 20, borderBottom: '1px solid #334155', paddingBottom: 1 }}>
        {TABS.map(t => (
          <button key={t.key} onClick={() => setTab(t.key)} style={{
            background: 'none', border: 'none', cursor: 'pointer',
            padding: '8px 16px', fontSize: 14, fontWeight: tab === t.key ? 700 : 400,
            color: tab === t.key ? '#38bdf8' : '#64748b',
            borderBottom: tab === t.key ? '2px solid #38bdf8' : '2px solid transparent',
            marginBottom: -1,
          }}>{t.label}</button>
        ))}
      </div>

      {tab === 'overstock' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <ReportSummary title="Overstock by Severity" data={overstockChart} xKey="severity" yKey="count" colorByIndex loading={loading} />
          <OverstockTable data={overstock} loading={loading} />
        </div>
      )}

      {tab === 'turnover' && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          <ReportSummary title="Top 10 Products by Turnover Ratio" data={turnoverChart} xKey="id" yKey="ratio" loading={loading} />
          <TurnoverTable data={turnover} loading={loading} />
        </div>
      )}

      {tab === 'suppliers' && <SupplierView data={suppliers} loading={loading} />}
    </div>
  );
}

function OverstockTable({ data, loading }) {
  const SEV = { HIGH: '#ef4444', MEDIUM: '#f59e0b', LOW: '#38bdf8' };
  return (
    <div style={{ background: '#1e293b', borderRadius: 10, border: '1px solid #334155', overflow: 'hidden' }}>
      <div style={{ padding: '12px 16px', borderBottom: '1px solid #334155', fontSize: 14, fontWeight: 600, color: '#f1f5f9' }}>
        Overstock Details
      </div>
      {loading ? <div style={{ padding: 24, textAlign: 'center', color: '#64748b' }}>Loading…</div> : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                {['Product', 'Store', 'Date', 'Stock', 'Reorder Pt', 'Excess', 'Value (₹)', 'Severity'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontSize: 12, color: '#94a3b8', fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((r, i) => (
                <tr key={i} style={{ borderBottom: '1px solid #0f172a' }}>
                  <td style={{ padding: '8px 14px', color: '#38bdf8' }}>{r.product_id}</td>
                  <td style={{ padding: '8px 14px', color: '#94a3b8' }}>{r.store_id}</td>
                  <td style={{ padding: '8px 14px', color: '#64748b' }}>{r.report_date}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right' }}>{r.current_stock_units}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: '#64748b' }}>{r.reorder_point_units}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: '#fbbf24' }}>{r.excess_units ?? '—'}</td>
                  <td style={{ padding: '8px 14px', textAlign: 'right', color: '#94a3b8' }}>
                    {r.excess_value ? Number(r.excess_value).toLocaleString('en-IN') : '—'}
                  </td>
                  <td style={{ padding: '8px 14px' }}>
                    <span style={{ color: SEV[r.overstock_severity] || '#94a3b8', fontWeight: 700, fontSize: 12 }}>
                      {r.overstock_severity}
                    </span>
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

function TurnoverTable({ data, loading }) {
  return (
    <div style={{ background: '#1e293b', borderRadius: 10, border: '1px solid #334155', overflow: 'hidden' }}>
      <div style={{ padding: '12px 16px', borderBottom: '1px solid #334155', fontSize: 14, fontWeight: 600, color: '#f1f5f9' }}>
        Turnover Details
      </div>
      {loading ? <div style={{ padding: 24, textAlign: 'center', color: '#64748b' }}>Loading…</div> : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
            <thead>
              <tr style={{ borderBottom: '1px solid #334155' }}>
                {['Product', 'Store', 'Category', 'Month', 'Units Sold', 'Revenue (₹)', 'Avg Inv', 'Ratio', 'Days Held'].map(h => (
                  <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontSize: 12, color: '#94a3b8', fontWeight: 600 }}>{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.map((r, i) => {
                const ratio = parseFloat(r.turnover_ratio || 0);
                return (
                  <tr key={i} style={{ borderBottom: '1px solid #0f172a' }}>
                    <td style={{ padding: '8px 14px', color: '#38bdf8' }}>{r.product_id}</td>
                    <td style={{ padding: '8px 14px', color: '#94a3b8' }}>{r.store_id}</td>
                    <td style={{ padding: '8px 14px', color: '#818cf8' }}>{r.category}</td>
                    <td style={{ padding: '8px 14px', color: '#64748b' }}>{r.report_month}</td>
                    <td style={{ padding: '8px 14px', textAlign: 'right' }}>{r.total_units_sold}</td>
                    <td style={{ padding: '8px 14px', textAlign: 'right', color: '#94a3b8' }}>
                      {Number(r.total_revenue).toLocaleString('en-IN')}
                    </td>
                    <td style={{ padding: '8px 14px', textAlign: 'right', color: '#64748b' }}>{r.avg_inventory ?? '—'}</td>
                    <td style={{ padding: '8px 14px', textAlign: 'right', fontWeight: 700,
                      color: ratio < 2 ? '#f59e0b' : ratio > 10 ? '#22c55e' : '#f1f5f9' }}>
                      {r.turnover_ratio ?? '—'}
                    </td>
                    <td style={{ padding: '8px 14px', textAlign: 'right', color: '#64748b' }}>{r.days_inventory_held ?? '—'}</td>
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
