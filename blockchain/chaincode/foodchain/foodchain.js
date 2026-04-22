// foodchain.js

const { Contract } = require('fabric-contract-api');

class FoodChainContract extends Contract {
    async RecordSensorData(ctx, batchId, sensorData) {
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        await ctx.stub.putState(batchKey, Buffer.from(JSON.stringify(sensorData)));
        await this.emitEvent(ctx, 'SensorDataRecorded', { batchId, sensorData });
    }

    async GetBatchHistory(ctx, batchId) {
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        const data = await ctx.stub.getState(batchKey);
        return data ? JSON.parse(data.toString()) : null;
    }

    async GetCurrentStatus(ctx, batchId) {
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        const data = await ctx.stub.getState(batchKey);
        if (data) {
            const sensorData = JSON.parse(data.toString());
            return sensorData.currentStatus;
        }
        return null;
    }

    async GetAllBatches(ctx) {
        const iterator = await ctx.stub.getStateByRange('', '');
        const allBatches = [];
        while (true) {
            const res = await iterator.next();
            if (res.done) break;
            allBatches.push(JSON.parse(res.value.value.toString()));
        }
        return allBatches;
    }

    async emitEvent(ctx, eventName, payload) {
        const buffer = Buffer.from(JSON.stringify(payload));
        await ctx.stub.setEvent(eventName, buffer);
    }
}

module.exports = FoodChainContract;