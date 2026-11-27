import { useState, useEffect } from 'react';
import SupplierView from '../components/SupplierView';
import { fetchSupplierPerformance, fetchSuppliers } from '../services/api';

export default function SuppliersPage() {
  const [perf, setPerf]       = useState([]);
  const [suppliers, setSup]   = useState([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState('');

  useEffect(() => {
    Promise.all([
      fetchSupplierPerformance({ limit: 100 }),
      fetchSuppliers(),
    ]).then(([p, s]) => {
      setPerf(p.data);
      setSup(s.data);
    }).finally(() => setLoading(false));
  }, []);

  const filtered = selected ? perf.filter(r => r.supplier_id === selected) : perf;

  return (
    <div>
      <h2 style={{ fontSize: 20, fontWeight: 700, marginBottom: 20, color: '#f1f5f9' }}>Supplier Management</h2>

      {/* Supplier cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 12, marginBottom: 24 }}>
        {suppliers.slice(0, 10).map(s => (
          <div key={s.supplier_id} onClick={() => setSelected(selected === s.supplier_id ? '' : s.supplier_id)}
            style={{
              background: selected === s.supplier_id ? '#1e3a5f' : '#1e293b',
              border: `1px solid ${selected === s.supplier_id ? '#3b82f6' : '#334155'}`,
              borderRadius: 8, padding: '14px 16px', cursor: 'pointer',
            }}>
            <div style={{ fontWeight: 600, color: '#38bdf8', fontSize: 13 }}>{s.supplier_id}</div>
            <div style={{ color: '#f1f5f9', fontSize: 12, marginTop: 2 }}>{s.supplier_name}</div>
            <div style={{ color: '#64748b', fontSize: 11, marginTop: 4 }}>
              {s.country} · {s.lead_time_days}d lead
            </div>
            <div style={{ marginTop: 6 }}>
              <span style={{
                background: s.is_active ? '#14532d' : '#450a0a',
                color: s.is_active ? '#86efac' : '#fca5a5',
                borderRadius: 4, padding: '1px 7px', fontSize: 10, fontWeight: 600,
              }}>{s.is_active ? 'ACTIVE' : 'INACTIVE'}</span>
            </div>
          </div>
        ))}
      </div>

      {/* Performance table */}
      <SupplierView data={filtered} loading={loading} />
    </div>
  );
}
