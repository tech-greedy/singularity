import React, { useEffect, useState, ChangeEvent, FormEvent } from 'react';
import Slider from 'rc-slider';
import 'rc-slider/assets/index.css';

interface Preparation {
  id: string;
  name: string;
  path: string;
  outDir: string;
}

interface FormValues {
  datasetName: string;
  datasetPath: string;
  outDir: string;
  tmpDir: string;
  dealSize: number;
}

const PreparationManagement: React.FC = () => {
  const [preparations, setPreparations] = useState<Preparation[]>([]);
  const [selectedPreparations, setSelectedPreparations] = useState<string[]>([]);
  const [progress, setProgress] = useState(0);
  const [isProgressModalVisible, setProgressModalVisible] = useState(false);
  const [isCreateModalVisible, setCreateModalVisible] = useState(false);
  const [formValues, setFormValues] = useState<FormValues>({
    datasetName: '',
    datasetPath: '',
    outDir: '',
    tmpDir: '',
    dealSize: 32,
  });

  const handleInputChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { name, value } = event.target;
    setFormValues(prev => ({ ...prev, [name]: value }));
  };

  const handleSliderChange = (value: number | number[]) => {
    if (typeof value === 'number') {
      setFormValues(prev => ({ ...prev, dealSize: value }));
    }
  };
  
  const handleSubmit = async (event: FormEvent) => {
    console.log('formValues', formValues);
    event.preventDefault();
    try {
      await fetch('http://localhost:7001/preparations', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formValues),
      });
      setCreateModalVisible(false);
      // Fetch the preparations again to reflect the newly created preparation
      await fetchPreparations();
    } catch (error) {
      console.error('Error creating a preparation:', error);
    }
  };

  const handleSelectAll = (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      setSelectedPreparations(preparations.map(p => p.id));
    } else {
      setSelectedPreparations([]);
    }
  };

  const handleSelectOne = (id: string) => (event: React.ChangeEvent<HTMLInputElement>) => {
    if (event.target.checked) {
      setSelectedPreparations(prev => [...prev, id]);
    } else {
      setSelectedPreparations(prev => prev.filter(item => item !== id));
    }
  };

  const updatePreparations = async (action: string) => {
    if(selectedPreparations.length === 0) return alert('Please select at least one preparation');
    setProgress(0);
    setProgressModalVisible(true);
    for (let i = 0; i < selectedPreparations.length; i++) {
      const id = selectedPreparations[i];
      await fetch(`http://localhost:7001/preparation/${id}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action: action }),
      });
      setProgress((i + 1) / selectedPreparations.length);
    }
  };

  async function fetchPreparations() {
    const res = await fetch('http://localhost:7001/preparations');
    const data = await res.json();
    setPreparations(data);
  }

  useEffect(() => {
    fetchPreparations();
  }, []);


  return (
    <div className="App">
      <div className="flex justify-start mb-4">
        <button className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded ml-2 mr-2" onClick={() => setCreateModalVisible(true)}>Create</button>
        <button className="bg-red-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded mr-2" onClick={() => updatePreparations('pause')}>Pause</button>
        <button className="bg-green-500 hover:bg-green-700 text-white font-bold py-2 px-4 rounded" onClick={() => updatePreparations('resume')}>Resume</button>
      </div>
      {isProgressModalVisible && (
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
                <button type="button" className="mt-3 w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-blue-500 text-base font-medium text-white hover:bg-blue-700 focus:outline-none sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm" onClick={() => setProgressModalVisible(false)}>Close</button>
              </div>
            </div>
          </div>
        </div>
      )}

      {isCreateModalVisible && (
        <div className="fixed z-10 inset-0 overflow-y-auto" aria-labelledby="modal-title" role="dialog" aria-modal="true">
          <div className="flex items-center justify-center min-h-screen pt-4 px-4 pb-20 text-center sm:block sm:p-0">
            <div className="fixed inset-0 transition-opacity" aria-hidden="true">
              <div className="absolute inset-0 bg-gray-500 opacity-75"></div>
            </div>
            <span className="hidden sm:inline-block sm:align-middle sm:h-screen" aria-hidden="true">&#8203;</span>
            <div className="inline-block align-middle bg-white rounded-lg text-left overflow-hidden shadow-xl transform sm:align-middle sm:max-w-lg sm:w-full">
              <div className="bg-white px-4 pt-5 pb-4 sm:p-6 sm:pb-4">
                <div className="sm:flex sm:items-start">
                  <div className="mt-3 text-center sm:mt-0 sm:text-left w-full">
                    <h3 className="text-lg leading-6 font-medium text-gray-900" id="modal-title">Create Preparation</h3>
                    <div className="mt-2">
                      <form id="myForm" onSubmit={handleSubmit} className="bg-white rounded px-8 pt-6 pb-8 mb-4">
                        <div className="mb-4">
                          <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="datasetName">
                            Dataset Name
                          </label>
                          <input className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline" id="datasetName" type="text" placeholder="Dataset Name" name="datasetName" value={formValues.datasetName} onChange={handleInputChange} required />
                        </div>
                        <div className="mb-4">
                          <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="datasetPath">
                            Dataset Path
                          </label>
                          <input className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline" id="datasetPath" type="text" placeholder="Dataset Path" name="datasetPath" value={formValues.datasetPath} onChange={handleInputChange} required />
                        </div>
                        <div className="mb-4">
                          <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="outDir">
                            Out Directory
                          </label>
                          <input className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline" id="outDir" type="text" placeholder="Out Directory" name="outDir" value={formValues.outDir} onChange={handleInputChange} required />
                        </div>
                        <div className="mb-4">
                          <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="tmpDir">
                            Temp Directory
                          </label>
                          <input className="shadow appearance-none border rounded w-full py-2 px-3 text-gray-700 leading-tight focus:outline-none focus:shadow-outline" id="tmpDir" type="text" placeholder="Temp Directory" name="tmpDir" value={formValues.tmpDir} onChange={handleInputChange} />
                        </div>
                        <div className="mb-4">
                          <label className="block text-gray-700 text-sm font-bold mb-2" htmlFor="dealSize">
                            Deal Size
                          </label>
                          <div className="flex items-center justify-between">
                            <span className="text-gray-700 text-sm">32 GiB</span>
                            <Slider min={32} max={64} defaultValue={formValues.dealSize} step={32} onChange={handleSliderChange} className='w-70' />
                            <span className="text-gray-700 text-sm">64 GiB</span>
                          </div>
                        </div>
                      </form>
                    </div>
                  </div>
                </div>
              </div>
              <div className="bg-gray-50 px-4 py-3 sm:px-6 sm:flex sm:flex-row-reverse">
                <button type="button" className="ml-3 inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-blue-500 text-base font-medium text-white hover:bg-blue-700 focus:outline-none sm:mt-0 sm:ml-3 sm:w-auto sm:text-sm" onClick={() => setCreateModalVisible(false)}>Close</button>
                <button type="submit" form="myForm" className="w-full inline-flex justify-center rounded-md border border-transparent shadow-sm px-4 py-2 bg-green-500 text-base font-medium text-white hover:bg-green-700 focus:outline-none sm:mt-0 sm:w-auto sm:text-sm">
                  Submit
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              <input type="checkbox" onChange={handleSelectAll} checked={selectedPreparations.length === preparations.length && preparations.length > 0} />
            </th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Name</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Path</th>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Output Directory</th>
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {preparations.map((preparation) => (
            <tr key={preparation.id}>
              <td className="px-6 py-4 whitespace-nowrap">
                <input type="checkbox" checked={selectedPreparations.includes(preparation.id)} onChange={handleSelectOne(preparation.id)} />
              </td>
              <td className="px-6 py-4 whitespace-nowrap">{preparation.id}</td>
              <td className="px-6 py-4 whitespace-nowrap">{preparation.name}</td>
              <td className="px-6 py-4 whitespace-nowrap">{preparation.path}</td>
              <td className="px-6 py-4 whitespace-nowrap">{preparation.outDir}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default PreparationManagement;
