const config = require('config');
const { MongoClient } = require('mongodb');

const logger = require('../logging');
const PromCollector = require('../metrics/PromCollector');
let client;

// Used for working with local data
if (config.mongodb.uri) {
    logger.info('[Mongodb] Using %o', config.mongodb);
    client = new MongoClient(config.mongodb.uri);

}
if (!config.mongodb.db || !config.mongodb.collection) {
    console.log('Missing db or collection name');
}

const db = client.db(config.mongodb.db);
const collection = db.collection(config.mongodb.collection);

// make sure insert only one
collection.createIndex(
    {
        conferenceId: 1,
        conferenceUrl: 1,
        dumpId: 1,
        baseDumpId: 1,
        userId: 1,
        app: 1,
        sessionId: 1,
        startDate: 1,
        endDate: 1
    },
    { unique: true }
);

const getDumpId = ({ clientId }) => `${clientId}.gz`;

/**
 *
 * @param {*} data
 */
async function saveEntry(collectionName, { ...entry }) {

    try {
        const result = await db.collection(collectionName).insertOne(entry);

        logger.info('[Mongodb] Saved metadata %o, %o', entry, result);

        return true;
    } catch (error) {
        if (error?.writeError?.code === 11000) {
            logger.warn('[Mongodb] duplicate entry: %o; error: %o', entry, error);

            return false;
        }
        PromCollector.mongodbErrorCount.inc();

        logger.error('[Mongodb] Error saving data %o, %o', entry, error);

        return true;
    }
}

/**
 *
 * @param {*} data
 */
async function saveEntryAssureUnique({ ...data }) {


    const { clientId } = data;
    const [ baseClientId, order ] = clientId.split('_');

    data.baseDumpId = baseClientId;

    let saveSuccessful = false;
    let clientIdIncrement = Number(order) || 0;
    let entry = {};

    while (!saveSuccessful) {
        try {
            entry = Object.assign(data, {
                dumpId: getDumpId(data),
                conferenceId: data.conferenceId.toLowerCase(),
                conferenceUrl: data.conferenceUrl?.toLowerCase()
            });
        } catch (error) {
            logger.error('[Mongodb] Error saving metadata %o, %o', data, error);
        }

        saveSuccessful = await saveEntry(config.mongodb.collection, entry);

        if (!saveSuccessful) {
            logger.warn('[Mongodb] duplicate cliendId %s, incrementing reconnect count', data.clientId);
            data.clientId = `${baseClientId}_${++clientIdIncrement}`;
        }
    }

    return data.clientId;
}


module.exports = {
    saveEntryAssureUnique,
    saveEntry
};
