const Block = require('../models/blocks')
const { parentPort, workerData } = require('worker_threads');
const { Web3 } = require("web3");
const rpcUrl = process.env.RPC_URL;
const { produce } = require('../services/kafkaConfig');
const httpProvider = new Web3.providers.HttpProvider(rpcUrl);
const web3 = new Web3(httpProvider);

//slow performance -- excluded
async function getBlockWithCache(blockNumber) {
    const client = require('../services/config');
    require('../services/databaseConfig')

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




getBlockWithCache(workerData.blockNumber)
    .then((result) => {
        console.log("Result --", result)
        parentPort.postMessage(result);
    })
    .catch((error) => {
        parentPort.postMessage({ error: error.message });
    });