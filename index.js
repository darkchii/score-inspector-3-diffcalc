const { Op } = require('sequelize');
const { AltScoreLive, CheckConnection, Databases, AltBeatmapLive } = require('./helpers/db');
const { default: axios } = require('axios');
require('dotenv').config();

const SCORES_PER_BATCH = 1000;
const BATCH_FETCH = 10;
const DEV_USER_ID = -1; //If not -1, it will only process scores from this user (good for testing random scores but same profile)

async function requestData(score) {
    const url = process.env.NODE_ENV === 'development' ? process.env.DIFF_CALC_URL_DEV : process.env.DIFF_CALC_URL;

    const beatmapId = score.beatmap_id;
    const mods = score.mods;
    const rulesetId = score.ruleset_id;

    const postData = {
        beatmap_id: beatmapId,
        mods: mods,
        ruleset_id: rulesetId,
    }

    try {
        const response = await axios.post(`http://${url}/attributes`, postData, {
            timeout: 15000 //15 seconds timeout
        });
        return response.data;
    } catch (error) {
        console.error(`Error requesting data for score ID ${score.id}:`, error.message);
        throw error;
    }
}

async function countMissingScores() {
    const count = await AltScoreLive.count({
        where: {
            ...(DEV_USER_ID !== -1 ? { user_id: DEV_USER_ID } : {}),
            [Op.or]: {
                modded_sr: null,
                attr_diff: null,
                attr_recalc: true,
                [Op.and]: { //fixing faulty calculations that get through somehow
                    attr_diff: { [Op.ne]: null },
                    ['attr_diff.star_rating']: 0,
                    ['attr_diff.max_combo']: 0,
                    '$BeatmapLive.max_combo$': { [Op.gt]: 0 }
                }
            }
        },
        include: [{ model: AltBeatmapLive }]
    });
    console.log(`[DIFF-CALC] Found ${count.toFixed(0)} scores missing diff calculations.`);
    return count;
}

let avgTimePerBatch = []; //array of {totalTime: number, batchCount: number}, remove old entries after 50 batches to keep a recent average
async function processScores(totalMissing) {
    let timeFetch = Date.now();
    const scores = await AltScoreLive.findAll({
        where: {
            ...(DEV_USER_ID !== -1 ? { user_id: DEV_USER_ID } : {}),
            [Op.or]: {
                modded_sr: null,
                attr_diff: null,
                attr_recalc: true,
                [Op.and]: { //fixing faulty calculations that get through somehow
                    attr_diff: { [Op.ne]: null },
                    ['attr_diff.star_rating']: 0,
                    ['attr_diff.max_combo']: 0,
                    '$BeatmapLive.max_combo$': { [Op.gt]: 0 }
                }
            }
        },
        order: [['beatmap_id_fk', 'ASC']],
        include: [{ model: AltBeatmapLive }],
        limit: SCORES_PER_BATCH
    });

    if (scores.length === 0) {
        //pause time :D
        console.log('[DIFF-CALC] No scores found for processing. Taking a small break.');
        //sleep for 30 seconds
        await new Promise(resolve => setTimeout(resolve, 30000));
        return;
    }
    let timeFetchElapsed = Date.now() - timeFetch;

    // Process scores in batches to speed up
    let timeCalc = Date.now();
    const dataMap = new Map();

    for (let i = 0; i < scores.length; i += BATCH_FETCH) {
        const batch = scores.slice(i, i + BATCH_FETCH);
        await Promise.all(batch.map(async (score) => {
            const scoreId = score.id;
            try {
                const data = await requestData(score);
                if (!data || Object.keys(data).length === 0) {
                    console.error(`[DIFF-CALC] Received empty data for score ID ${scoreId}. Skipping update.`);
                    return;
                }

                dataMap.set(scoreId, data);
            } catch (error) {
                console.error(`[DIFF-CALC] Error processing score ID ${scoreId}:`, error);
            }
        }));
    }

    let timeCalcElapsed = Date.now() - timeCalc;

    let timeSubmit = Date.now();

    const values = Array.from(dataMap.entries()).map(([scoreId, data]) => {
        const attrDiffStr = JSON.stringify(data);
        return `(${scoreId}, '${attrDiffStr.replace(/'/g, "''")}'::jsonb, ${data.star_rating})`;
    }).join(', ');

    const updateQuery = `
        UPDATE scorelive
        SET attr_diff = v.attr_diff, attr_recalc = false, modded_sr = v.star_rating
        FROM (VALUES ${values}) AS v(id, attr_diff, star_rating)
        WHERE scorelive.id = v.id;
    `;

    try {
        await Databases.osuAlt.transaction(async (t) => {
            await Databases.osuAlt.query(updateQuery, { transaction: t });
        });
    } catch (error) {
        console.error('[DIFF-CALC] Error updating scores in the database:', error);
    }

    let timeSubmitElapsed = Date.now() - timeSubmit;
    let elapsed = timeCalcElapsed + timeSubmitElapsed + timeFetchElapsed;
    avgTimePerBatch.push({ totalTime: elapsed, batchCount: 1 });
    if (avgTimePerBatch.length > 50) {
        avgTimePerBatch.shift();
    }

    let avgTime = avgTimePerBatch.reduce((sum, entry) => sum + entry.totalTime, 0) / avgTimePerBatch.reduce((sum, entry) => sum + entry.batchCount, 0);
    let remainingBatches = Math.ceil((totalMissing - scores.length) / SCORES_PER_BATCH);
    let expectedTime = remainingBatches * avgTime;
    //show expected time as hh:mm:ss
    let hours = Math.floor(expectedTime / 3600000);
    let minutes = Math.floor((expectedTime % 3600000) / 60000);
    let seconds = Math.floor((expectedTime % 60000) / 1000);
    let formattedExpectedTime = `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
    console.log(`[DIFF-CALC] Processed ${scores.length.toFixed(0)} scores in ${elapsed}ms (${((scores.length / elapsed) * 1000).toFixed(2)} scores/second) - Fetch: ${timeFetchElapsed}ms, Calculation: ${timeCalcElapsed}ms, Save: ${timeSubmitElapsed}ms, Time left: ${formattedExpectedTime}`);
}

let recountScoresIndex = 0;
let totalMissing = 0;
async function main() {
    console.log('[DIFF-CALC] Welcome to scores inspector difficulty calculator...');
    console.log('[DIFF-CALC] It will automatically start!');

    totalMissing = await countMissingScores();
    while (true) {
        try {
            if (recountScoresIndex >= 10) {
                totalMissing = await countMissingScores();
                recountScoresIndex = 0;
            }
            await processScores(totalMissing);
            recountScoresIndex++;
        } catch (error) {
            console.error('[DIFF-CALC] Unexpected error in main loop:', error);
            console.log('[DIFF-CALC] Waiting for 30 seconds before retrying...');
            await new Promise(resolve => setTimeout(resolve, 30000));
        }
    }
}

main();