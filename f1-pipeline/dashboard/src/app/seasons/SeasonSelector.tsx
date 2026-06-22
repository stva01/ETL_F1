"use client";

import { useRouter, useSearchParams } from 'next/navigation';
import { useEffect, useState } from 'react';

export default function SeasonSelector({ seasons, currentYear }: { seasons: number[], currentYear: number }) {
  const router = useRouter();
  const [selectedYear, setSelectedYear] = useState(currentYear);

  return (
    <div className="card" style={{ padding: '10px 20px', display: 'flex', alignItems: 'center', gap: '20px', background: 'var(--red)', color: '#fff' }}>
        <h3 style={{ margin: 0, textTransform: 'uppercase', letterSpacing: '2px', fontSize: '18px' }}>Select Season:</h3>
        <select 
          className="form-control" 
          value={selectedYear} 
          onChange={(e) => {
            setSelectedYear(parseInt(e.target.value));
            router.push(`/seasons?year=${e.target.value}`);
          }}
          style={{ padding: '8px 12px', fontSize: '16px', background: '#fff', color: '#111', border: 'none', borderRadius: '4px', cursor: 'pointer', fontFamily: "'Barlow', sans-serif", fontWeight: 'bold' }}
        >
            {seasons.map(y => (
                <option key={y} value={y}>{y} Season</option>
            ))}
        </select>
    </div>
  );
}
