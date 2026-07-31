'use strict';

const { Contract } = require('fabric-contract-api');

class FoodChainContract extends Contract {

    async InitLedger(ctx) {
        console.info('FoodChain Ledger Initialized');
    }

    async RecordSensorData(ctx, batchId, sensorDataJSON) {
        const sensorData = JSON.parse(sensorDataJSON);

        // Input validation
        if (!sensorData.temperature && sensorData.temperature !== 0) {
            throw new Error('Missing required field: temperature');
        }
        if (!sensorData.humidity && sensorData.humidity !== 0) {
            throw new Error('Missing required field: humidity');
        }
        if (!sensorData.current_stage) {
            throw new Error('Missing required field: current_stage');
        }

        const validStages = ['field', 'warehouse', 'transport', 'retailer', 'consumer'];
        if (!validStages.includes(sensorData.current_stage)) {
            throw new Error(`Invalid stage: ${sensorData.current_stage}. Must be one of: ${validStages.join(', ')}`);
        }

        // Role-based access control via MSP
        const clientMSP = ctx.clientIdentity.getMSPID();
        sensorData.recordedBy = clientMSP;
        sensorData.recordedAt = new Date().toISOString();
        sensorData.txId = ctx.stub.getTxID();

        // Compliance check — smart contract enforced thresholds
        const thresholds = {
            field: { temp: [18, 27], humidity: [65, 85] },
            warehouse: { temp: [4, 10], humidity: [70, 90] },
            transport: { temp: [5, 12], humidity: [60, 80] },
            retailer: { temp: [6, 14], humidity: [55, 75] },
            consumer: { temp: [8, 16], humidity: [50, 70] }
        };

        const limits = thresholds[sensorData.current_stage];
        const alerts = [];
        if (sensorData.temperature < limits.temp[0] || sensorData.temperature > limits.temp[1]) {
            alerts.push(`Temperature ${sensorData.temperature}°C out of range [${limits.temp[0]}-${limits.temp[1]}]`);
        }
        if (sensorData.humidity < limits.humidity[0] || sensorData.humidity > limits.humidity[1]) {
            alerts.push(`Humidity ${sensorData.humidity}% out of range [${limits.humidity[0]}-${limits.humidity[1]}]`);
        }

        sensorData.complianceAlerts = alerts;
        sensorData.isCompliant = alerts.length === 0;

        // Store with composite key for efficient queries
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        const stageKey = ctx.stub.createCompositeKey('batch~stage', [batchId, sensorData.current_stage]);
        const timestampKey = ctx.stub.createCompositeKey('batch~time', [batchId, sensorData.recordedAt]);

        const dataBuffer = Buffer.from(JSON.stringify(sensorData));
        await ctx.stub.putState(batchKey, dataBuffer);
        await ctx.stub.putState(stageKey, dataBuffer);
        await ctx.stub.putState(timestampKey, dataBuffer);

        // Emit event for real-time listeners
        ctx.stub.setEvent('SensorDataRecorded', Buffer.from(JSON.stringify({
            batchId,
            stage: sensorData.current_stage,
            isCompliant: sensorData.isCompliant,
            txId: sensorData.txId,
            temperature: sensorData.temperature,
            humidity: sensorData.humidity
        })));

        return JSON.stringify(sensorData);
    }

    async GetBatchData(ctx, batchId) {
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        const data = await ctx.stub.getState(batchKey);
        if (!data || data.length === 0) {
            throw new Error(`Batch ${batchId} not found`);
        }
        return data.toString();
    }

    async GetBatchHistory(ctx, batchId) {
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        const iterator = await ctx.stub.getHistoryForKey(batchKey);
        const history = [];

        while (true) {
            const res = await iterator.next();
            if (res.done) break;

            const record = {
                txId: res.value.txId,
                timestamp: res.value.timestamp,
                isDelete: res.value.isDelete
            };

            try {
                record.value = JSON.parse(res.value.value.toString());
            } catch (err) {
                record.value = res.value.value.toString();
            }

            history.push(record);
        }

        await iterator.close();
        return JSON.stringify(history);
    }

    async GetBatchesByStage(ctx, stage) {
        const iterator = await ctx.stub.getStateByPartialCompositeKey('batch~stage', []);
        const results = [];

        while (true) {
            const res = await iterator.next();
            if (res.done) break;

            const attrs = ctx.stub.splitCompositeKey(res.value.key);
            if (attrs.attributes[1] === stage) {
                try {
                    results.push(JSON.parse(res.value.value.toString()));
                } catch (err) {
                    results.push(res.value.value.toString());
                }
            }
        }

        await iterator.close();
        return JSON.stringify(results);
    }

    async TransferBatch(ctx, batchId, fromStage, toStage) {
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        const data = await ctx.stub.getState(batchKey);
        if (!data || data.length === 0) {
            throw new Error(`Batch ${batchId} not found`);
        }

        const batch = JSON.parse(data.toString());
        if (batch.current_stage !== fromStage) {
            throw new Error(`Batch is at ${batch.current_stage}, not ${fromStage}`);
        }

        batch.current_stage = toStage;
        batch.transferredAt = new Date().toISOString();
        batch.transferTxId = ctx.stub.getTxID();

        await ctx.stub.putState(batchKey, Buffer.from(JSON.stringify(batch)));

        ctx.stub.setEvent('BatchTransferred', Buffer.from(JSON.stringify({
            batchId, fromStage, toStage, txId: batch.transferTxId
        })));

        return JSON.stringify(batch);
    }

    async GetAllBatches(ctx) {
        const iterator = await ctx.stub.getStateByPartialCompositeKey('batch', []);
        const batches = [];

        while (true) {
            const res = await iterator.next();
            if (res.done) break;
            // Only include primary batch keys (not stage or time composites)
            const attrs = ctx.stub.splitCompositeKey(res.value.key);
            if (attrs.objectType === 'batch' && attrs.attributes.length === 1) {
                try {
                    batches.push(JSON.parse(res.value.value.toString()));
                } catch (err) {
                    batches.push(res.value.value.toString());
                }
            }
        }

        await iterator.close();
        return JSON.stringify(batches);
    }

    async BatchExists(ctx, batchId) {
        const batchKey = ctx.stub.createCompositeKey('batch', [batchId]);
        const data = await ctx.stub.getState(batchKey);
        return data && data.length > 0;
    }
}

module.exports = FoodChainContract;