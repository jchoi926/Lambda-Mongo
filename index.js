const AWS = require('aws-sdk');
const Promise = require('bluebird');
const mongo = require('mongodb').MongoClient;

let mongoClient;
let mongoDB;
let config = {};

exports.handler = handler;

function handler(event, context, callback) {
	const payload = JSON.parse(event.Records[0].Sns.Message);
	if (payload.config)
		config = payload.config;

	initialize()
		.then(() => processDrafts(payload.userId, payload.resourceData))
		.then(res => {
			console.log('Mongo upserted', res.result);
			callback(null, 'success');
		})
		.catch(err => {
			console.log('Mongo upsert error', err.message);
			callback(err);
		})
	;
};

/**
 * Bootstrap Lambda
 * @return {Promise}
 */
function initialize() {
	const configPromise = Object.keys(config).length > 0
		? Promise.resolve(config)
		: getConfigParams()
	;

	return configPromise
		.then(() => getMongoClient())
	;
}

/**
 * Set Mongo Client
 * @return {Promise}
 */
function getMongoClient() {
	if (mongoClient)
		return Promise.resolve(mongoClient);

	return new Promise((resolve, reject) => {

		mongo.connect(getMongoConnectionString(), (err, client) => {
			if (err) {
				return reject(err);
			}

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

		mongoDB.collection('drafts').updateOne(
			{'user_id': userId, 'itemId': resourceData.Id},
			{$set: resourceObject},
			{upsert: true},
			function (err, res) {
				if (err) {
					reject(err);
				} else {
					resolve(res);
				}
			}
		);
	});
}

/**
 * Get parameters from AWS Systems Manager Parameter Store
 * @return {Promise}
 */
function getConfigParams() {
	if (Object.keys(config).length > 0)
		return Promise.resolve(config);

	const S3 = Promise.promisifyAll(new AWS.S3({region: 'us-east-1'}));
	return S3.getObjectAsync({Bucket: 'ci-office-notification', Key: `${process.env.ENV}.json`})
		.then(s3Obj => {
			config = JSON.parse(s3Obj.Body.toString());
			return config;
		})
	;
}
