const express = require('express')
const cors = require('cors')
require("dotenv").config();
const PORT = 5000
const cluster = require('cluster')
const totalCPUs = require('os').cpus().length

if (cluster.isMaster) {

    console.log(`Total Number of CPU Counts is ${totalCPUs}`);

    for (var i = 0; i < 8; i++) {
        cluster.fork();
    }
    cluster.on("online", worker => {
        console.log(`Worker Id is ${worker.id} and PID is ${worker.process.pid}`);
    });
    cluster.on("exit", worker => {
        console.log(`Worker Id ${worker.id} and PID is ${worker.process.pid} is offline`);
        console.log("Let's fork new worker!");
        cluster.fork();
    });
}
else {

    require('./services/databaseConfig')
    const { consume } = require('./services/kafkaConfig')
    const blockRoutes = require('./routes/blocks')

    const app = express()
    app.use(cors({ origin: '*' }))
    app.options('*', cors());
    app.use(express.json())

    app.use('/blocks', blockRoutes)
    app.get('/', (req, res) => {
        return res.json("Server is running")
    })

    app.listen(PORT, () => {
        console.log("Server is running on PORT ", PORT)
        consume('store-block')
    })
}


