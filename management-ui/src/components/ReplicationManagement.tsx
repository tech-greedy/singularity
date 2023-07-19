import React, { useEffect, useState } from 'react';

interface Replication {
  id: string;
  storageProviders: string;
  client: string;
  startDelay: number;
  duration: number;
  status: string;
  notes: string;
}

const ReplicationManagement: React.FC = () => {
  const [replications, setReplications] = useState<Replication[]>([]);
  const [selectedReplications, setSelectedReplications] = useState<string[]>([]);
  const [progress, setProgress] = useState(0);
  const [isModalVisible, setModalVisible] = useState(false);

  const handleSelectAll = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      setSelectedReplications(replications.map(p => p.id));
    } else {
      setSelectedReplications([]);
    }
  };

  const handleSelectOne = (id: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      setSelectedReplications(prev => [...prev, id]);
    } else {
      setSelectedReplications(prev => prev.filter(item => item !== id));
    }
  };

  const updateReplications = async (action: string) => {
    if(selectedReplications.length === 0) return alert('Please select at least one replication');
    setProgress(0);
    setModalVisible(true);
    for (let i = 0; i < selectedReplications.length; i++) {
      const id = selectedReplications[i];
      await fetch(`http://localhost:7004/replications/${id}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action }),
      });
      setProgress((i + 1) / selectedReplications.length);
    }
  };

  useEffect(() => {
    fetch('http://localhost:7004/replications')
      .then((response) => response.json())
      .then((data) => setReplications(data))
      .catch((error) => console.error(error));
  }, []);

  return (
    <div className="App">
      <div className="flex justify-start mb-4">
        <button className="bg-red-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded mr-2 ml-2" onClick={() => updateReplications('pause')}>Pause</button>
        <button className="bg-blue-500 hover:bg-green-700 text-white font-bold py-2 px-4 rounded mr-2" onClick={() => updateReplications('resume')}>Resume</button>
        <button className="bg-green-500 hover:bg-green-700 text-white font-bold py-2 px-4 rounded" onClick={() => updateReplications('complete')}>Complete</button>
      </div>
      {isModalVisible && (
        <div className="fixed z-10 inset-0 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
          <div className="flex items-end justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
            <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" aria-hidden="true"></div>
            <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
            <div className="inline-block align-bottom bg-white rounded-lg text-left overflow-hidden shadow-xl transform sm:align-middle sm:max-w-lg sm:w-full">
              <div className="px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
                <h3 className="text-lg leading-6 font-medium text-gray-900" id="modal-title">Progress</h3>
                <div className="mt-2">
                  <div className="h-4 w-full bg-gray-200 rounded">
                    <div className="h-full text-center text-xs text-white bg-blue-500" style={{ width: `${progress * 100}%` }}></div>
                  </div>
                </div>
              </div>
              <div className="px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
                <button type="button" className="mt-3 w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-blue-500 text-base font-medium text-white hover:bg-blue-700 focus:outline-none sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm" onClick={() => setModalVisible(false)}>Close</button>
              </div>
            </div>
          </div>
        </div>
      )}
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              <input type="checkbox" onChange={handleSelectAll} checked={selectedReplications.length === replications.length && replications.length > 0} />
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Storage Providers</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Client</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Start Delay</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Duration</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Notes</th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {replications.map((replication) => (
            <tr key={replication.id}>
              <td className="px-6 py-4 whitespace-nowrap">
                <input type="checkbox" checked={selectedReplications.includes(replication.id)} onChange={handleSelectOne(replication.id)} />
              </td>
              <td className="px-6 py-4 whitespace-nowrap">{replication.id}</td>
              <td className="px-6 py-4 whitespace-nowrap">{replication.storageProviders}</td>
              <td className="px-6 py-4 whitespace-nowrap">{replication.client}</td>
              <td className="px-6 py-4 whitespace-nowrap">{replication.startDelay}</td>
              <td className="px-6 py-4 whitespace-nowrap">{replication.duration}</td>
              <td className="px-6 py-4 whitespace-nowrap">{replication.status}</td>
              <td className="px-6 py-4 whitespace-nowrap">{replication.notes}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default ReplicationManagement;
