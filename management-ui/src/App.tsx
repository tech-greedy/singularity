import React from 'react';
import { Tab, Tabs, TabList, TabPanel } from 'react-tabs';
import 'react-tabs/style/react-tabs.css';

import PreparationManagement from './components/PreparationManagement';
import ReplicationManagement from './components/ReplicationManagement';

const App: React.FC = () => {
  return (
    <div className="App">
      <Tabs>
        <TabList>
          <Tab>Preparation Management</Tab>
          <Tab>Replication Management</Tab>
        </TabList>

        <TabPanel>
          <PreparationManagement />
        </TabPanel>
        <TabPanel>
          <ReplicationManagement />
        </TabPanel>
      </Tabs>
    </div>
  );
};

export default App;
