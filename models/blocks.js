const mongoose = require('mongoose');

const blockSchema = new mongoose.Schema(
    {
        blockNumber: {
            type: String,
            trim: true,
            unique: true,
            index: true,
        },
        data: {
            type: String,
            trim: true
        }
    }
);

module.exports = mongoose.model('Block', blockSchema);
