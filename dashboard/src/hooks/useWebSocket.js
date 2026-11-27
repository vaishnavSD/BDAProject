import { useEffect, useRef, useState, useCallback } from 'react';

const WS_URL = process.env.REACT_APP_WS_URL || 'ws://localhost:8000';

export function useWebSocket() {
  const [alerts, setAlerts]       = useState([]);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(`${WS_URL}/alerts/ws/alerts`);
    wsRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      // Send keepalive ping every 30s
      reconnectTimer.current = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) ws.send('ping');
      }, 30000);
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (Array.isArray(data)) {
          setAlerts(prev => {
            const existingIds = new Set(prev.map(a => a.alert_id));
            const fresh = data.filter(a => !existingIds.has(a.alert_id));
            // Keep the 100 most recent
            return [...fresh, ...prev].slice(0, 100);
          });
        }
      } catch (_) {}
    };

    ws.onclose = () => {
      setConnected(false);
      clearInterval(reconnectTimer.current);
      // Auto-reconnect after 5s
      setTimeout(connect, 5000);
    };

    ws.onerror = () => ws.close();
  }, []);

  useEffect(() => {
    connect();
    return () => {
      clearInterval(reconnectTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  const dismiss = useCallback((alertId) => {
    setAlerts(prev => prev.filter(a => a.alert_id !== alertId));
  }, []);

  return { alerts, connected, dismiss };
}
