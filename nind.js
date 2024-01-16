const express = require('express');
const app = express();
const CosmosClient = require("@azure/cosmos").CosmosClient;
const cors = require('cors');
const https=require('https');
const fs=require('fs');
const path=require('path');
const axios = require('axios');
const bodyParser = require('body-parser');


//process.env.NODE_TLS_REJECT_UNAUTHORIZED = '0';

app.use(cors());

app.use(bodyParser.json());
//const options = {
//        key: fs.readFileSync('/etc/letsencrypt/live/factacyinsights.in/privkey.pem'),
//        cert: fs.readFileSync('/etc/letsencrypt/live/factacyinsights.in/fullchain.pem')
//  };


app.use( express.json() );

app.get('/', (req,res) =>{
	res.status(200).send('Request Recieved');
});

app.post('/createapi/:p1/:p2/:p3/:p4', (req, res) => {


try{

	console.log('In block 1');

	function removeUndefinedKeys(obj) {
 	 for (const key in obj) {
    	if (obj.hasOwnProperty(key) && obj[key] === ':undefined') {
      	delete obj[key];
    }
  }
}
	
	let pobj = req.params;
	
	removeUndefinedKeys(pobj);
	
	console.log(pobj);
	
	const endpoint = req.query.endpoint;
	
	const dbname = req.query.dbname;
	
	const containername = req.query.cont;
	
	let dbquery = req.query.dbquery;
	
	//const data = req.body;
	
	
	let inpstr = '';

	for (const key in pobj) {
  		if (pobj.hasOwnProperty(key)) {
    		const value = pobj[key];
		console.log(typeof(value));
		const tmp = value.slice(1);
		inpstr = inpstr + ` const ${tmp} = req.query.${tmp} ; `;
		let rstr = '${' + `${tmp}` + '}'
    		dbquery = dbquery.replace('{}', rstr);
		console.log(dbquery);
  }
}
	
	console.log(dbquery);
	
	console.log(data);
	console.log(typeof(data));



	let btemp = `app.get('${endpoint}', (req, res) => { (async(sterm) => { try{ ${inpstr} const data = req.body; console.log(data); console.log(typeof(data));  const key = process.env.COSMOS_KEY; const endpoint = process.env.COSMOS_ENDPOINT;  var x = { endpoint, key }; const cosmosClient = new CosmosClient( x ); const databaseName = '${dbname}'; const { database } = await cosmosClient.databases.createIfNotExists({ id: databaseName });console.log('database ready'); const containerName = '${containername}'; const { container } = await database.containers.createIfNotExists({ id: containerName }); console.log('container ready');  const querySpec3 = { query: \`${dbquery}\` }; const { resources } = await container.items.query(querySpec3).fetchAll(); console.log(resources);  const uniq = [...new Array(resources)]; uniq_new = uniq[0];
res.send(uniq_new); } catch(err){ console.log('Error'); console.log(err); } })();});`;
	
	console.log(btemp);
} catch(err){
	console.log(err);
}



	try{
        //console.log(req.body);
	
	//const ema = req.body.ema;
	
	//console.log(ema);
	
	fs.appendFile('b-test.js', '\n\n' , (err) => { console.log(err); });
	
	fs.appendFile('b-test.js', btemp , function (err) {
	
  	if (err) throw err;
  	console.log('Saved!');
		
	});
	
        res.status(200).send('Successfully Recieved request');
		
	}catch(err) {
		console.log(err);
	};
});

const port = 5000;


app.listen(port, () => {
  console.log('Server listening on port ' + port);
});

