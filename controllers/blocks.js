const Block = require('../models/blocks')
const { Web3 } = require("web3");
const rpcUrl = process.env.RPC_URL
const client = require('../services/config')
const { produce } = require('../services/kafkaConfig')
const httpProvider = new Web3.providers.HttpProvider(rpcUrl);
const web3 = new Web3(httpProvider);
const { Worker } = require('worker_threads')
const totalCPUs = require('os').cpus().length

//using worker start ---------------------------
async function createWorker(blockNumbers) {
    return new Promise((resolve, reject) => {
        const worker = new Worker(__dirname + '/../workers/blockWorker.js', {
            workerData: { blockNumbers }
        })
        worker.on('message', (data) => {
            worker.terminate()
            resolve(data)
        })
        worker.on('error', (error) => {
            worker.terminate()
            reject(error)
        })
    })
}

//parallel processing
exports.fetchBlockUsingWorker = async (req, res) => {
    try {
        const { start_block, limit } = req.body
        const start = Date.now();
        const blockNumbers = Array.from({ length: parseInt(limit) }, (_, i) => parseInt(start_block) + i);
        let blocksFetchPromises = []
        let total = limit / totalCPUs
        for (let i = 0; i < totalCPUs; i++) {
            if (i != totalCPUs - 1) {
                let data = blockNumbers.slice(i * total, (i * total + total))
                blocksFetchPromises.push(createWorker(data))
            }
            else {
                let data = blockNumbers.slice(i * total)
                blocksFetchPromises.push(createWorker(data))
            }
        }
        const blocksData = await Promise.all(blocksFetchPromises)

        const end = Date.now();

        console.log("Total Time Taken: ", (end - start) / 1000);

        return res.json({
            code: 'success',
            timeTaken: (end - start) / 1000,
            blocksData: blocksData.flat(),
        });
    } catch (error) {
        console.log("Error --", error);
        return res.json({
            code: 'failed'
        });
    }
}

//using worker End ---------------------------




//using parralel processing start ---------------------------

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

//using parralel processing end ---------------------------


//using sequential processing start ---------------------------

exports.fetchBlockDataSequential = async (req, res) => {
    try {
        const { start_block, limit } = req.body
        const start = Date.now();
        let blocks = []
        for (let i = 0; i < parseInt(limit); i++) {
            const blockData = await web3.eth.getBlock(parseInt(start_block));

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

//using sequential processing End ---------------------------
