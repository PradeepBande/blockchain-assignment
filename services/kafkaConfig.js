const { Kafka, Partitioners } = require("kafkajs");
const Block = require('../models/blocks')
const client = require('./config')

const kafka = new Kafka({
    clientId: "nodejs-kafka",
    brokers: ["192.168.1.106:9092"],
});

const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner })
const consumer = kafka.consumer({ groupId: "block-group" });

async function produce(topic, messages) {
    try {
        await producer.connect();
        await producer.send({
            topic: topic,
            messages: messages,
        });
    } catch (error) {
        console.error(error);
    } finally {
        await producer.disconnect();
    }
}

async function consume(topic) {
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: topic, fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const value = message.value.toString();
                const blockNumber = message.key.toString();
                const cacheKey = `block:${blockNumber}`;
                // console.log("value --", value)
                // console.log("key --", message.key.toString())

                const cachedData = await client.get(cacheKey);
                if (!cachedData)
                    client.set(cacheKey, value);

                let isPresent = await Block.findOne({ blockNumber })
                if (!isPresent) {
                    let newBlock = new Block({ blockNumber, data: value })
                    newBlock.save()
                }

            },
        });
    } catch (error) {
        console.error(error);
    }
}


module.exports = { consumer, producer, consume, produce }
