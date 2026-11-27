import { useState } from 'react';
import { useStockSnapshot } from '../hooks/useInventory';
import StockLevelTable from '../components/StockLevelTable';

export default function InventoryPage() {
  const [belowReorder, setBelowReorder] = useState(false);
  const [storeId, setStoreId]           = useState('');

  const { data, loading } = useStockSnapshot({
    below_reorder_only: belowReorder,
    store_id: storeId || undefined,
    limit: 500,
  });

  const stores = [...new Set(data.map(r => r.store_id))].sort();

  return (
    <div>
      <h2 style={{ fontSize: 20, fontWeight: 700, marginBottom: 20, color: '#f1f5f9' }}>Inventory Stock Levels</h2>

      <div style={{ display: 'flex', gap: 12, marginBottom: 20, alignItems: 'center', flexWrap: 'wrap' }}>
        <select value={storeId} onChange={e => setStoreId(e.target.value)} style={selectStyle}>
          <option value="">All Stores</option>
          {stores.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 13, color: '#94a3b8', cursor: 'pointer' }}>
          <input type="checkbox" checked={belowReorder} onChange={e => setBelowReorder(e.target.checked)} />
          Below reorder point only
        </label>
        <span style={{ marginLeft: 'auto', fontSize: 13, color: '#64748b' }}>{data.length} records</span>
      </div>

      <StockLevelTable data={data} loading={loading} />
    </div>
  );
}

const selectStyle = {
  background: '#0f172a', border: '1px solid #334155', borderRadius: 6,
  color: '#e2e8f0', padding: '6px 10px', fontSize: 13, outline: 'none', cursor: 'pointer',
};
