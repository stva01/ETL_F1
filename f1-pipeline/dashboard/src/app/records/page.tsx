import { querySnowflake } from "@/lib/snowflake";

export const revalidate = 0;

interface RecordItem {
  record_category: string;
  record_type: string;
  holder_name: string;
  record_value_formatted: string;
  context_note: string | null;
}

export default async function RecordsPage() {
  const allRecords = await querySnowflake<RecordItem>(`
    SELECT * FROM MARTS.mart_records 
    ORDER BY record_category, record_type
  `);

  const champCats = ['Most Championship Titles'];
  const raceCats = ['Most Race Wins', 'Youngest Race Winner', 'Most Wins in a Season', 'Most Podiums', 'Highest Win Rate'];
  const qualCats = ['Most Pole Positions', 'Most Fastest Laps', 'Most Points in a Season', 'Most Race Starts'];

  const champRecords = allRecords.filter(r => champCats.includes(r.record_category));
  const raceRecords = allRecords.filter(r => raceCats.includes(r.record_category));
  const qualRecords = allRecords.filter(r => qualCats.includes(r.record_category));

  // Render a single record item like mockdashboard
  const renderRecordItem = (r: RecordItem, badgeColor: string) => (
    <div className="prog-item" style={{ marginTop: '12px' }} key={r.record_category + r.record_type + r.holder_name}>
      <div className="prog-label">
        <span className="prog-name">{r.record_type}</span>
        <span className={`prog-val badge ${badgeColor}`}>{r.record_value_formatted}</span>
      </div>
      <div style={{ fontSize: '12px', color: 'var(--text-dim)', marginTop: '2px' }}>
        {r.holder_name}
        {r.context_note && <span style={{ marginLeft: '4px', fontStyle: 'italic' }}>({r.context_note})</span>}
      </div>
    </div>
  );

  return (
    <main className="section active" id="records">
        <div className="sec-head">
            <h2>F1 <span>Records</span> & Milestones</h2>
            <div className="sec-head-line"></div>
        </div>

        <div className="grid-3" style={{ marginBottom: '20px' }}>
            <div className="card">
                <div className="card-title">🏆 Championship Records</div>
                <div className="progress-row" style={{ display: 'flex', flexDirection: 'column' }}>
                  {champRecords.map((r, i) => renderRecordItem(r, i % 2 === 0 ? 'badge-gold' : 'badge-red'))}
                </div>
            </div>
            
            <div className="card">
                <div className="card-title">🏁 Race Records</div>
                <div className="progress-row" style={{ display: 'flex', flexDirection: 'column' }}>
                  {raceRecords.map((r, i) => renderRecordItem(r, ['badge-red', 'badge-gold', 'badge-green', 'badge-dim'][i % 4]))}
                </div>
            </div>

            <div className="card">
                <div className="card-title">📊 Qualifying & Points</div>
                <div className="progress-row" style={{ display: 'flex', flexDirection: 'column' }}>
                  {qualRecords.map((r, i) => renderRecordItem(r, ['badge-red', 'badge-gold', 'badge-green', 'badge-dim'][i % 4]))}
                </div>
            </div>
        </div>

        <div className="card">
            <div className="card-title">Complete World Champions History — 1950–2024</div>
            <div style={{ overflowX: 'auto' }}>
                <table className="f1-table">
                    <thead>
                        <tr>
                            <th>Year</th>
                            <th>Driver Champion</th>
                            <th>Nationality</th>
                            <th>Constructor</th>
                            <th>Wins</th>
                            <th>Pts</th>
                        </tr>
                    </thead>
                    <tbody>
                      <tr>
                        <td colSpan={6} style={{ textAlign: 'center', padding: '20px', color: 'var(--text-dim)' }}>
                          See the Seasons tab for comprehensive yearly history.
                        </td>
                      </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </main>
  );
}
