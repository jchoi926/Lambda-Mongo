let mongoClient;
let mongoDB;
let config = {};

exports.handler = (event, context, callback) => {
	console.log("EVENT", event);
	console.log("RECORDS", event.Records);
	console.log("RECORD", event.Records[0]);
	console.log("PAYLOAD", event.Records[0].Sns);
	const payload = JSON.parse(event.Records[0].Sns.Message);
	config = payload.config;
	getMongoClient()
		.then(() => processDrafts(payload.userId, payload.resourceData))
		.then(res => {
			console.log("RES", res);
			callback(null, 'success');
		})
		.catch(err => {
			console.log("ERR", 'error');
			callback(err);
		})
	;
};

/**
 * Set Mongo Client
 * @return {Promise}
 */
function getMongoClient() {
	if (mongoClient)
		return Promise.resolve(mongoClient);

	return new Promise((resolve, reject) => {
		const mongo = require('mongodb').MongoClient;
		mongo.connect(getMongoConnectionString(), (err, client) => {
			if (err) {
				console.log('Mongo connection error', err);
				return reject(err);
			}

			console.log('Mongo client connected');
			mongoClient = client;
			mongoDB = client.db(config.mongo.db);
			return resolve(mongoClient);
		});
	});
}

/**
 * Build mongo connection string
 * @return {string}
 */
function getMongoConnectionString() {
	return `mongodb://${encodeURIComponent(config.mongo.user)}:${encodeURIComponent(config.mongo.pass)}@${config.mongo.host}/${config.mongo.db}?${config.mongo.connectOptions}&replicaSet=${config.mongo.replicaSet}&ssl=true&${config.mongo.authOptions}`;
}

/**
 * Get parameters from AWS Systems Manager Parameter Store
 * @return {Promise}
 */
function getConfigParams() {
	if (Object.keys(config).length > 0)
		return Promise.resolve(config);

	const S3 = Promise.promisifyAll(new AWS.S3({region: 'us-east-1'}));
	return S3.getObjectAsync({Bucket: 'ci-office-notification', Key: `${env}.json`})
		.then(s3Obj => {
			config = JSON.parse(s3Obj.Body.toString());
			return config;
		})
	;
}

/**
 * Cycle through User's Drafts and process if target draft exist
 * @param {String} userId
 * @param {Object} resourceData
 */
function processDrafts(userId, resourceData) {
    return new Promise((resolve, reject) => {
        let resourceObject = {};
    	Object.keys(resourceData).forEach(propName => {
    		if (!propName.startsWith('@') && !propName.startsWith('_')) {
    			const newPropName = (propName === 'Id') ? 'item' + propName : propName;
    			resourceObject[newPropName] = resourceData[propName];
    		}
    	});
    	console.log('RESOURCE OBJECT TO UPDATE', resourceObject);
    	mongoDB.collection('drafts').updateOne(
    		{'user_id': userId, 'itemId': resourceData.Id},
    		{$set: resourceObject},
    		{upsert: true},
    		function (err, res) {
    			if (err) {
    			    reject();
    				console.log("DRAFT UPDATE ERROR", err);
    			} else {
    			    resolve();
    				console.log("DRAFT FOUND AND UPDATED");
    			}
    		}
    	);
    });
}
