import { useSummary } from '../hooks/useInventory';
import { useWebSocket } from '../hooks/useWebSocket';
import StatCard from '../components/StatCard';
import AlertsBanner from '../components/AlertsBanner';
import { useStockSnapshot, useOverstock } from '../hooks/useInventory';
import StockLevelTable from '../components/StockLevelTable';
import ReportSummary from '../components/ReportSummary';

export default function Dashboard() {
  const { data: summary, loading: sumLoading } = useSummary();
  const { alerts, dismiss } = useWebSocket();
  const { data: snapshot, loading: snapLoading } = useStockSnapshot({ below_reorder_only: false, limit: 200 });
  const { data: overstock } = useOverstock({ limit: 10 });

  // Aggregate overstock by severity for chart
  const overstockChart = ['HIGH', 'MEDIUM', 'LOW'].map(s => ({
    severity: s,
    count: overstock.filter(r => r.overstock_severity === s).length,
  })).filter(d => d.count > 0);

  // Category distribution from snapshot
  const categoryMap = {};
  snapshot.forEach(r => {
    // We only have product_id here; group by first 3 chars as proxy for category code
    const key = r.product_id;
    categoryMap[key] = (categoryMap[key] || 0) + r.closing_stock;
  });

  return (
    <div>
      {/* Live alerts */}
      <AlertsBanner alerts={alerts} onDismiss={dismiss} />

      {/* KPI cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 16, marginBottom: 24 }}>
        <StatCard label="Total Products"       value={sumLoading ? '…' : summary?.total_products}       icon="📦" color="#38bdf8" />
        <StatCard label="Active Stores"        value={sumLoading ? '…' : summary?.total_stores}         icon="🏪" color="#818cf8" />
        <StatCard label="Suppliers"            value={sumLoading ? '…' : summary?.total_suppliers}      icon="🚛" color="#34d399" />
        <StatCard label="Unresolved Alerts"    value={sumLoading ? '…' : summary?.unresolved_alerts}    icon="🔔" color="#f97316"
          sub={summary?.critical_alerts ? `${summary.critical_alerts} critical` : null} />
        <StatCard label="Stockout Risk"        value={sumLoading ? '…' : summary?.stockout_risk_products} icon="⚠️" color="#ef4444" />
        <StatCard label="Overstock Products"   value={sumLoading ? '…' : summary?.overstock_products}   icon="📈" color="#fbbf24" />
      </div>

      {/* Charts row */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <ReportSummary
          title="Overstock by Severity"
          data={overstockChart}
          xKey="severity" yKey="count"
          colorByIndex
        />
        <ReportSummary
          title="Stock Movements Today (top products)"
          data={snapshot.slice(0, 10).map(r => ({ id: r.product_id, out: r.total_outbound }))}
          xKey="id" yKey="out"
        />
      </div>

      {/* Stock table */}
      <StockLevelTable data={snapshot} loading={snapLoading} />
    </div>
  );
}
