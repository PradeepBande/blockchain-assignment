const Redis = require('redis')

const client = Redis.createClient(process.env.REDIS_URL)

client.on('error', err => console.log('Redis Client Error', err));

client.connect();

module.exports = client