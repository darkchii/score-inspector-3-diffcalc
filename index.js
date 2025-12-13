const { Op } = require('sequelize');
const { AltScoreLive, CheckConnection, Databases } = require('./helpers/db');
const { default: axios } = require('axios');
require('dotenv').config();

const SCORES_PER_BATCH = 500;
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

async function processScores() {
    console.log('Starting score processing batch...');
    //TODO: Fetch scores that need diff calculation
    const scores = await AltScoreLive.findAll({
        where: {
            ...(DEV_USER_ID !== -1 ? { user_id: DEV_USER_ID } : {}),
            [Op.or]: {
                attr_diff: null,
                attr_recalc: true
            }
        },
        limit: SCORES_PER_BATCH
    });

    if (scores.length === 0) {
        //pause time :D
        console.log('[DIFF-CALC] No scores found for processing. Taking a small break.');
        //sleep for 30 seconds
        await new Promise(resolve => setTimeout(resolve, 30000));
        return;
    }

    console.log(`[DIFF-CALC] Fetched ${scores.length} scores to process.`);


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
        return `(${scoreId}, '${attrDiffStr.replace(/'/g, "''")}'::jsonb)`
    }).join(', ');

    const updateQuery = `
        UPDATE scorelive
        SET attr_diff = v.attr_diff, attr_recalc = false
        FROM (VALUES ${values}) AS v(id, attr_diff)
        WHERE scorelive.id = v.id;
    `;

    try {
        await Databases.osuAlt.transaction(async (t) => {
            await Databases.osuAlt.query(updateQuery, { transaction: t });
        });
        console.log('[DIFF-CALC] Successfully updated scores in the database.');
    } catch (error) {
        console.error('[DIFF-CALC] Error updating scores in the database:', error);
    }

    let timeSubmitElapsed = Date.now() - timeSubmit;
    let elapsed = timeCalcElapsed + timeSubmitElapsed;
    console.log(`[DIFF-CALC] Processed ${scores.length} scores in ${elapsed}ms (${((scores.length / elapsed) * 1000).toFixed(2)} scores/second) - Calculation: ${timeCalcElapsed}ms, Save: ${timeSubmitElapsed}ms`);

    console.log('[DIFF-CALC] Score processing batch completed.');
}

async function main() {
    console.log('[DIFF-CALC] Welcome to scores inspector difficulty calculator...');
    console.log('[DIFF-CALC] It will automatically start!');

    while (true) {
        try {
            await processScores();
        } catch (error) {
            console.error('[DIFF-CALC] Unexpected error in main loop:', error);
            console.log('[DIFF-CALC] Waiting for 30 seconds before retrying...');
            await new Promise(resolve => setTimeout(resolve, 30000));
        }
    }
}

main();