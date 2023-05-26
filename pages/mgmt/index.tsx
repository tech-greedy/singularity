import React from 'react'
import { GetStaticProps, NextPage } from 'next';
import Datastore from '../../src/common/Datastore';

const Home: NextPage<any> = ({ data }) => {
  return (
    <div className="container mx-auto">
      <h1 className="text-2xl font-bold mb-4">Replication Requests</h1>
      <ul>
        {data.map((item) => (
          <li key={item._id}>{item.client}</li>
        ))}
      </ul>
    </div>
  );
};

export const getStaticProps: GetStaticProps<any> = async () => {
  const res = await fetch('http://127.0.0.1:7006/api/replicationrequests');
  const data = await res.json();

  return {
    props: { data }, // will be passed to the page component as props
  };
};

export default Home;