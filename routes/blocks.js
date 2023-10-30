const express = require('express');
const { fetchBlockData, fetchBlockDataSequential } = require('../controllers/blocks');
const router = express.Router();


router.post('/get', fetchBlockData)

//testing purpose
router.post('/get/block', fetchBlockDataSequential)


module.exports = router; 