import { useState, useEffect, useCallback } from 'react';
import {
  fetchSummary, fetchStockSnapshot,
  fetchOverstockReport, fetchAlerts,
} from '../services/api';

export function useSummary() {
  const [data, setData]     = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError]   = useState(null);

  const load = useCallback(async () => {
    try {
      setLoading(true);
      const res = await fetchSummary();
      setData(res.data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);
  return { data, loading, error, reload: load };
}

export function useStockSnapshot(params = {}) {
  const [data, setData]       = useState([]);
  const [loading, setLoading] = useState(true);
  const key = JSON.stringify(params);

  useEffect(() => {
    setLoading(true);
    fetchStockSnapshot(params)
      .then(r => setData(r.data))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [key]); // eslint-disable-line

  return { data, loading };
}

export function useOverstock(params = {}) {
  const [data, setData]       = useState([]);
  const [loading, setLoading] = useState(true);
  const key = JSON.stringify(params);

  useEffect(() => {
    setLoading(true);
    fetchOverstockReport(params)
      .then(r => setData(r.data))
      .catch(() => setData([]))
      .finally(() => setLoading(false));
  }, [key]); // eslint-disable-line

  return { data, loading };
}

export function useAlerts(params = {}) {
  const [data, setData]       = useState({ total: 0, items: [] });
  const [loading, setLoading] = useState(true);
  const key = JSON.stringify(params);

  const load = useCallback(() => {
    setLoading(true);
    fetchAlerts(params)
      .then(r => setData(r.data))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [key]); // eslint-disable-line

  useEffect(() => { load(); }, [load]);
  return { data, loading, reload: load };
}
