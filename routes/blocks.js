const express = require('express');
const { fetchBlockData, fetchBlockDataSequential, fetchBlockUsingWorker } = require('../controllers/blocks');
const router = express.Router();

//Parallel processing
router.post('/get', fetchBlockData)

//sequential processing
router.post('/get/block', fetchBlockDataSequential)

//Parallel processing using workes/ multithreading
router.post('/get-by-workers', fetchBlockUsingWorker)

module.exports = router; 