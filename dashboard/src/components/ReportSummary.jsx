import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';

const COLORS = ['#38bdf8','#818cf8','#34d399','#fb923c','#f472b6',
                '#a78bfa','#4ade80','#facc15','#60a5fa','#f87171'];

export default function ReportSummary({ title, data = [], xKey, yKey, loading = false, colorByIndex = false }) {
  return (
    <div style={{ background: '#1e293b', borderRadius: 10, border: '1px solid #334155', padding: 20 }}>
      <div style={{ fontWeight: 600, fontSize: 14, color: '#f1f5f9', marginBottom: 16 }}>{title}</div>
      {loading ? (
        <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#64748b' }}>
          Loading…
        </div>
      ) : data.length === 0 ? (
        <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#64748b' }}>
          No data available
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={200}>
          <BarChart data={data} margin={{ top: 4, right: 8, left: 0, bottom: 4 }}>
            <XAxis dataKey={xKey} tick={{ fill: '#64748b', fontSize: 11 }} axisLine={false} tickLine={false} />
            <YAxis tick={{ fill: '#64748b', fontSize: 11 }} axisLine={false} tickLine={false} />
            <Tooltip
              contentStyle={{ background: '#0f172a', border: '1px solid #334155', borderRadius: 6, fontSize: 12 }}
              labelStyle={{ color: '#94a3b8' }}
              itemStyle={{ color: '#e2e8f0' }}
            />
            <Bar dataKey={yKey} radius={[4, 4, 0, 0]}>
              {data.map((_, i) => (
                <Cell key={i} fill={colorByIndex ? COLORS[i % COLORS.length] : '#38bdf8'} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
