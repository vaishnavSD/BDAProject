import axios from 'axios';

const BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 10000,
  headers: { 'Content-Type': 'application/json' },
});

// ── Products ──────────────────────────────────────────────────────────────────
export const fetchProducts    = (params = {}) => api.get('/products/', { params });
export const fetchProduct     = (id)          => api.get(`/products/${id}`);
export const fetchCategories  = ()            => api.get('/products/categories');

// ── Inventory ─────────────────────────────────────────────────────────────────
export const fetchSummary      = ()           => api.get('/inventory/summary');
export const fetchStores       = ()           => api.get('/inventory/stores');
export const fetchStockSnapshot = (params={}) => api.get('/inventory/snapshot', { params });
export const fetchLatestSnapshotDate = ()     => api.get('/inventory/snapshot/latest-date');

// ── Alerts ────────────────────────────────────────────────────────────────────
export const fetchAlerts       = (params={}) => api.get('/alerts/', { params });
export const resolveAlert      = (id)        => api.patch(`/alerts/${id}/resolve`);

// ── Reports ───────────────────────────────────────────────────────────────────
export const fetchOverstockReport       = (params={}) => api.get('/reports/overstock', { params });
export const fetchSupplierPerformance   = (params={}) => api.get('/reports/supplier-performance', { params });
export const fetchInventoryTurnover     = (params={}) => api.get('/reports/inventory-turnover', { params });
export const fetchSuppliers             = ()           => api.get('/reports/suppliers');

export default api;
