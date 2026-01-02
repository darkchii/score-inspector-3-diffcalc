const { Sequelize } = require('sequelize');
const AltScoreLiveModel = require('../models/AltScoreLiveModel');
const AltBeatmapLiveModel = require('../models/AltBeatmapLiveModel');
require('dotenv').config();

let databases = {
    osuAlt: new Sequelize(process.env.ALT_DB_DATABASE, process.env.ALT_DB_USER, process.env.ALT_DB_PASSWORD,
        {
            dialect: 'postgres',
            host: process.env.ALT_DB_HOST,
            port: process.env.ALT_DB_PORT,
            logging: false,
            retry: {
                max: 10
            }
        }
    )
};
module.exports.Databases = databases;

async function CheckConnection(database, timeout = 10000) {
    //just race between database.authenticate and a timeout
    let success = false;

    await Promise.race([
        database.authenticate().then(() => {
            success = true;
        }),
        new Promise((resolve, reject) => {
            setTimeout(() => {
                reject(new Error('Connection timeout'));
            }, timeout);
        })
    ]);

    if (!success) {
        throw new Error('Connection failed');
    }

    return success;
}

const AltScoreLive = AltScoreLiveModel(databases.osuAlt);
const AltBeatmapLive = AltBeatmapLiveModel(databases.osuAlt);

//each score has a beatmap,multiple scores can have the same beatmap
AltScoreLive.belongsTo(AltBeatmapLive, { foreignKey: 'beatmap_id', targetKey: 'beatmap_id' });

module.exports.CheckConnection = CheckConnection;
module.exports.AltScoreLive = AltScoreLive;
module.exports.AltBeatmapLive = AltBeatmapLive;
