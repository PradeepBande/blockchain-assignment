const Block = require('../models/blocks')
const { Web3 } = require("web3");
const rpcUrl = process.env.RPC_URL

const client = require('../services/config')
const { produce } = require('../services/kafkaConfig')

const httpProvider = new Web3.providers.HttpProvider(rpcUrl);
const web3 = new Web3(httpProvider);

async function getBlockWithCache(blockNumber) {
    const cacheKey = `block:${blockNumber}`;
    const cachedData = await client.get(cacheKey);

    if (cachedData) {
        return JSON.parse(cachedData);
    }
    console.log("====== Cache Missed ======")
    let data = await Block.findOne({ blockNumber })
    if (data) {
        client.set(cacheKey, data?.data);
        return JSON.parse(data?.data)
    }

    console.log("====== Database Missed ======")

    const blockData = await web3.eth.getBlock(blockNumber);
    let keys = Object.keys(blockData)
    for (let i = 0; i < keys.length; i++) {
        blockData[keys[i]] = Array.isArray(blockData[keys[i]]) ? blockData[keys[i]] : blockData[keys[i]].toString()
    }

    let message = [{ key: blockNumber.toString(), value: JSON.stringify(blockData) }]
    produce('store-block', message)
    return blockData;
}


//parallel processing
exports.fetchBlockData = async (req, res) => {
    try {
        const { start_block, limit } = req.body
        const start = Date.now();
        const blockNumbers = Array.from({ length: parseInt(limit) }, (_, i) => parseInt(start_block) + i);
        const blocksData = await Promise.all(blockNumbers.map(async (blockNumber) => getBlockWithCache(blockNumber)));

        const end = Date.now();

        console.log("Total Time Taken: ", (end - start) / 1000);

        return res.json({
            code: 'success',
            timeTaken: (end - start) / 1000,
            blocksData,
        });
    } catch (error) {
        console.log("Error --", error);
        return res.json({
            code: 'failed'
        });
    }
}



//testing purpose
exports.fetchBlockDataSequential = async (req, res) => {
    try {
        const { block_number, limit } = req.body
        const start = Date.now();
        let blocks = []
        for (let i = 0; i < parseInt(limit); i++) {
            const blockData = await web3.eth.getBlock(parseInt(block_number));

            let keys = Object.keys(blockData)
            let data = {}
            for (let i = 0; i < keys.length; i++) {
                blockData[keys[i]] = Array.isArray(blockData[keys[i]]) ? blockData[keys[i]] : blockData[keys[i]].toString()
                data = {
                    ...data,
                    [keys[i]]: Array.isArray(blockData[keys[i]]) ? blockData[keys[i]] : blockData[keys[i]].toString()
                }
            }
            blocks.push(blockData)
        }

        const end = Date.now();
        console.log("Total Time Taken: ", (end - start) / 1000);

        return res.json({
            code: 'success',
            timeTaken: (end - start) / 1000,
            total: blocks.length,
            blocks,
        });
    } catch (error) {
        console.log("Error --", error);
        return res.json({
            code: 'failed'
        });
    }
}



// function processBlockNumber(blockNumber) {
//     return new Promise((resolve, reject) => {
//         const worker = new Worker(__dirname + '/../workers/blockWorker.js', {
//             workerData: { blockNumber },
//         });

//         worker.on('message', (message) => {
//             if (message.error) {
//                 reject(new Error(message.error));
//             } else {
//                 // results.push(message);
//                 resolve(message);
//             }
//         });

//         worker.on('error', (error) => {
//             reject(error);
//         });
//     });
// }