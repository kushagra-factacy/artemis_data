const express = require('express');
const app = express();
const CosmosClient = require("@azure/cosmos").CosmosClient;
const cors = require('cors');
const https=require('https');
const fs=require('fs');
const path=require('path');
const axios = require('axios');

app.use(cors());

app.get('/etc', (req, res) => { console.log(req.body); }); app.get('/more', (req, res) => { res.send('Hi There'); console.log(req.body); }); 
app.get('/anotherone', (req, res) => { res.send('Hi There'); console.log(req.body); }); 


const port = 5001;


app.listen(port, () => {
  console.log('Server listening on port ' + port);
});

(async function() {
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
console.log(1);
await sleep(49000);
console.log(2);
process.exit(0);
})()

app.get('/allaicite', (req, res) => { (async(sterm) => { try{  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'cdb-L1'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'AICITE-IC'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'SELECT TOP 10 * FROM c' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/trsytdt', (req, res) => { (async(sterm) => { try{ const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'jhvvkjjhbnjvjhc'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'tgccfjkufvj'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'hgctgcyjhfc' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/trsytdt', (req, res) => { (async(sterm) => { try{ const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'jhvvkjjhbnjvjhc'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'tgccfjkufvj'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'hgctgcyjhfc' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/ytfvyjgb', (req, res) => { (async(sterm) => { try{ const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'gtchyjvukvk'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'uyvbkhn'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'hvbbkjjubgu' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/ytfvyjgb', (req, res) => { (async(sterm) => { try{ const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'gtchyjvukvk'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'uyvbkhn'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'hvbbkjjubgu' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/testnew', (req, res) => { (async(sterm) => { try{ const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'yvhjvkjbkjb'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'rtstyfjgukhbjb'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'ghcvjkbkj,bhj' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/caicite', (req, res) => { (async(sterm) => { try{ const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'cdb-L1'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'AICITE-IC'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'SECLECT {} * FROM c ORDER BY c._ts {}' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/caicite', (req, res) => { (async(sterm) => { try{  const lim = req.query.lim ;  const ord = req.query.ord ;  const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'cdb-L1'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'AICITE-IC'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: 'SECLECT {} * FROM c ORDER BY c._ts {}' }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});




app.get('/customfetch', (req, res) => { (async(sterm) => { try{  const attr = req.query.attr ;  const val = req.query.val ;  const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'heimdall-db'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'heimdall-v2'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: `SELECT * FROM c WHERE c.${attr} = ${val}` }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});




app.get('/getdeal', (req, res) => { (async(sterm) => { try{  const d_ids = req.query.d_ids ;  const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'heimdall-db'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'Deal-Id'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready'); console.log(d_ids); console.log(typeof(d_ids));  const querySpec3 = { query: `SELECT * FROM c WHERE c.id IN ( ${d_ids} )` }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});


app.get('/getarts1', (req, res) => { (async(sterm) => { try{  const art_id = req.query.art_id ;  const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'cdb-L1'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'AICITE-IC'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: `SELECT * FROM c WHERE c.Art_Id IN (${art_id})` }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

app.get('/summary', (req, res) => { (async(sterm) => { try{  const mca_cin = req.query.mca_cin ;  const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = 'heimdall-db'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = 'heimdall-v2'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: `SELECT TOP 100 * FROM c WHERE c.MCA_CIN = ${mca_cin}` }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});

