const { Op } = require('sequelize');
const { AltScoreLive, CheckConnection, Databases } = require('./helpers/db');
const { default: axios } = require('axios');
require('dotenv').config();

const SCORES_PER_BATCH = 100;
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
        order: [['beatmap_id', 'ASC']],
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
    let time = Date.now();
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

                score.attr_diff = data;
                score.attr_recalc = false;
            } catch (error) {
                console.error(`[DIFF-CALC] Error processing score ID ${scoreId}:`, error);
            }
        }));
    }
    
    const t = await Databases.osuAlt.transaction();
    try {
        for await (const score of scores) {
            await score.save({ transaction: t });
        }
        await t.commit();
    } catch (error) {
        console.error(`[DIFF-CALC] Error saving scores to database:`, error);
        await t.rollback();
    }

    let elapsed = Date.now() - time;
    console.log(`[DIFF-CALC] Processed ${scores.length} scores in ${elapsed}ms (${((scores.length / elapsed) * 1000).toFixed(2)} scores/second)`);
    
    console.log('[DIFF-CALC] Score processing batch completed.');
}

async function main() {
    console.log('[DIFF-CALC] Welcome to scores inspector difficulty calculator...');
    console.log('[DIFF-CALC] It will automatically start!');

    while (true) {
        try{
            await processScores();
        } catch (error) {
            console.error('[DIFF-CALC] Unexpected error in main loop:', error);
            console.log('[DIFF-CALC] Waiting for 30 seconds before retrying...');
            await new Promise(resolve => setTimeout(resolve, 30000));
        }
    }
}

main();