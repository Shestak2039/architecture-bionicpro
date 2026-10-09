import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

interface ReportItem {
  report_date: string;
  prosthesis_id: string;
  prosthesis_model: string;
  movements_count: number;
  avg_reaction_ms: number;
  max_reaction_ms: number;
  recognition_errors: number;
  avg_battery_level: number;
}

interface Report {
  user_id: string;
  available_until: string;
  period_truncated: boolean;
  items: ReportItem[];
}

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<Report | null>(null);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      await keycloak.updateToken(30);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      if (!response.ok) {
        const body = await response.json().catch(() => null);
        throw new Error(body?.detail || `Request failed with status ${response.status}`);
      }

      setReport(await response.json());
    } catch (err) {
      setReport(null);
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md">
        <h1 className="text-2xl font-bold mb-6">Usage Reports</h1>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Generating Report...' : 'Download Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}

        {report && (
          <div className="mt-6">
            <p className="mb-2 text-gray-700">
              User: {report.user_id}. Data available until {report.available_until}.
            </p>
            {report.items.length === 0 ? (
              <p className="text-gray-700">No data for this period.</p>
            ) : (
              <table className="text-sm border-collapse">
                <thead>
                  <tr className="bg-gray-100">
                    <th className="px-3 py-2 text-left">Date</th>
                    <th className="px-3 py-2 text-left">Prosthesis</th>
                    <th className="px-3 py-2 text-left">Model</th>
                    <th className="px-3 py-2 text-right">Movements</th>
                    <th className="px-3 py-2 text-right">Avg reaction, ms</th>
                    <th className="px-3 py-2 text-right">Max reaction, ms</th>
                    <th className="px-3 py-2 text-right">Recognition errors</th>
                    <th className="px-3 py-2 text-right">Avg battery, %</th>
                  </tr>
                </thead>
                <tbody>
                  {report.items.map((item) => (
                    <tr key={`${item.report_date}-${item.prosthesis_id}`} className="border-t">
                      <td className="px-3 py-1">{item.report_date}</td>
                      <td className="px-3 py-1">{item.prosthesis_id}</td>
                      <td className="px-3 py-1">{item.prosthesis_model}</td>
                      <td className="px-3 py-1 text-right">{item.movements_count}</td>
                      <td className="px-3 py-1 text-right">{item.avg_reaction_ms}</td>
                      <td className="px-3 py-1 text-right">{item.max_reaction_ms}</td>
                      <td className="px-3 py-1 text-right">{item.recognition_errors}</td>
                      <td className="px-3 py-1 text-right">{item.avg_battery_level}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
