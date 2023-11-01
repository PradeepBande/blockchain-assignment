const { parentPort, workerData } = require('worker_threads');
const { Web3 } = require("web3");

const Block = require('../models/blocks')
const { produce } = require('../services/kafkaConfig');

const rpcUrl = process.env.RPC_URL;
const httpProvider = new Web3.providers.HttpProvider(rpcUrl);
const web3 = new Web3(httpProvider);

const client = require('../services/config');
require('../services/databaseConfig')

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


async function main() {
    let promises = []
    for (let i = 0; i < workerData.blockNumbers.length; i++) {
        promises.push(getBlockWithCache(workerData.blockNumbers[i]))
    }

    let data = await Promise.all(promises)
    parentPort.postMessage(data)
}

main()