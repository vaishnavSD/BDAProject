import { Link, useLocation } from 'react-router-dom';
import { LayoutDashboard, Bell, BarChart3, Package, Truck } from 'lucide-react';

const NAV = [
  { to: '/',          label: 'Dashboard',  Icon: LayoutDashboard },
  { to: '/alerts',    label: 'Alerts',     Icon: Bell },
  { to: '/inventory', label: 'Inventory',  Icon: Package },
  { to: '/reports',   label: 'Reports',    Icon: BarChart3 },
  { to: '/suppliers', label: 'Suppliers',  Icon: Truck },
];

export default function Navbar({ alertCount = 0, wsConnected = false }) {
  const { pathname } = useLocation();

  return (
    <nav style={{
      position: 'fixed', top: 0, left: 0, right: 0, zIndex: 100,
      background: '#1e293b', borderBottom: '1px solid #334155',
      display: 'flex', alignItems: 'center',
      padding: '0 24px', height: 56,
    }}>
      {/* Brand */}
      <div style={{ fontWeight: 700, fontSize: 16, color: '#38bdf8', marginRight: 40, whiteSpace: 'nowrap' }}>
        📦 Smart Inventory
      </div>

      {/* Links */}
      <div style={{ display: 'flex', gap: 4, flex: 1 }}>
        {NAV.map(({ to, label, Icon }) => {
          const active = pathname === to || (to !== '/' && pathname.startsWith(to));
          return (
            <Link key={to} to={to} style={{
              display: 'flex', alignItems: 'center', gap: 6,
              padding: '6px 14px', borderRadius: 6, textDecoration: 'none',
              fontSize: 14, fontWeight: active ? 600 : 400,
              color: active ? '#38bdf8' : '#94a3b8',
              background: active ? 'rgba(56,189,248,0.1)' : 'transparent',
              transition: 'all 0.15s',
              position: 'relative',
            }}>
              <Icon size={15} />
              {label}
              {label === 'Alerts' && alertCount > 0 && (
                <span style={{
                  position: 'absolute', top: 2, right: 2,
                  background: '#ef4444', color: '#fff',
                  borderRadius: 999, fontSize: 10, fontWeight: 700,
                  padding: '0 5px', lineHeight: '16px', minWidth: 16,
                  textAlign: 'center',
                }}>{alertCount > 99 ? '99+' : alertCount}</span>
              )}
            </Link>
          );
        })}
      </div>

      {/* WS status */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: '#64748b' }}>
        <span style={{
          width: 8, height: 8, borderRadius: '50%',
          background: wsConnected ? '#22c55e' : '#ef4444',
          display: 'inline-block',
        }} />
        {wsConnected ? 'Live' : 'Offline'}
      </div>
    </nav>
  );
}
