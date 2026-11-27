import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Navbar from './components/Navbar';
import Dashboard from './pages/Dashboard';
import AlertsPage from './pages/AlertsPage';
import InventoryPage from './pages/InventoryPage';
import ReportsPage from './pages/ReportsPage';
import SuppliersPage from './pages/SuppliersPage';
import { useWebSocket } from './hooks/useWebSocket';

export default function App() {
  const { alerts, connected } = useWebSocket();

  return (
    <BrowserRouter>
      <Navbar alertCount={alerts.length} wsConnected={connected} />
      <main style={{ paddingTop: 72, paddingLeft: 24, paddingRight: 24, paddingBottom: 32, maxWidth: 1400, margin: '0 auto' }}>
        <Routes>
          <Route path="/"          element={<Dashboard />} />
          <Route path="/alerts"    element={<AlertsPage />} />
          <Route path="/inventory" element={<InventoryPage />} />
          <Route path="/reports"   element={<ReportsPage />} />
          <Route path="/suppliers" element={<SuppliersPage />} />
        </Routes>
      </main>
    </BrowserRouter>
  );
}
