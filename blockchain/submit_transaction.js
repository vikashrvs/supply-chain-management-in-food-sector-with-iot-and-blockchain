'use strict';
/**
 * FoodChain Fabric Gateway - Transaction Submitter
 * 
 * Connects to Hyperledger Fabric test-network (Org1)
 * and submits RecordSensorData transaction to foodchain chaincode.
 * 
 * Usage: node submit_transaction.js <batchId> <sensorDataJSON>
 * Output: JSON { success: true, txId: "..." } or { error: "..." }
 */

const { connect, signers } = require('@hyperledger/fabric-gateway');
const grpc = require('@grpc/grpc-js');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

// ── Fabric Network Config ────────────────────────────────────────────────────
const CHANNEL_NAME    = 'mychannel';
const CHAINCODE_NAME  = 'foodchain';
const MSP_ID          = 'Org1MSP';
const PEER_ENDPOINT   = 'localhost:7051';
const PEER_HOST_ALIAS = 'peer0.org1.example.com';

// ── Crypto paths (test-network Org1) ────────────────────────────────────────
const TEST_NETWORK_PATH = path.resolve(
    __dirname,
    'fabric-samples', 'test-network'
);

const CRYPTO_PATH = path.resolve(
    TEST_NETWORK_PATH,
    'organizations', 'peerOrganizations', 'org1.example.com'
);

const KEY_DIR_PATH = path.resolve(
    CRYPTO_PATH, 'users', 'User1@org1.example.com', 'msp', 'keystore'
);

const CERT_DIR_PATH = path.resolve(
    CRYPTO_PATH, 'users', 'User1@org1.example.com', 'msp', 'signcerts'
);

function getCertPath() {
    if (fs.existsSync(CERT_DIR_PATH)) {
        const files = fs.readdirSync(CERT_DIR_PATH).filter(f => f.endsWith('.pem'));
        if (files.length > 0) {
            return path.resolve(CERT_DIR_PATH, files[0]);
        }
    }
    return path.resolve(CERT_DIR_PATH, 'cert.pem');
}

const TLS_CERT_PATH = path.resolve(
    CRYPTO_PATH, 'peers', 'peer0.org1.example.com', 'tls', 'ca.crt'
);

// ── Main ────────────────────────────────────────────────────────────────────
async function main() {
    const args = process.argv.slice(2);
    const batchId      = args[0];
    const sensorDataJSON = args[1];

    if (!batchId || !sensorDataJSON) {
        output({ error: 'Usage: node submit_transaction.js <batchId> <sensorDataJSON>' });
        process.exit(1);
    }

    // Check crypto materials exist (i.e. network is running)
    if (!fs.existsSync(TLS_CERT_PATH)) {
        output({ error: 'Fabric network not running — TLS cert not found. Run network.sh up first.' });
        process.exit(1);
    }

    let client;
    let gateway;

    try {
        client  = await newGrpcConnection();
        gateway = connect({
            client,
            identity: await newIdentity(),
            signer:   await newSigner(),
            evaluateOptions: () => ({ deadline: Date.now() + 5000 }),
            endorseOptions:  () => ({ deadline: Date.now() + 15000 }),
            submitOptions:   () => ({ deadline: Date.now() + 5000 }),
            commitStatusOptions: () => ({ deadline: Date.now() + 60000 }),
        });

        const network  = gateway.getNetwork(CHANNEL_NAME);
        const contract = network.getContract(CHAINCODE_NAME);

        // Submit RecordSensorData transaction to the ledger
        const resultBytes = await contract.submitTransaction(
            'RecordSensorData',
            batchId,
            sensorDataJSON
        );

        const resultJSON = Buffer.from(resultBytes).toString('utf8');
        let result = {};
        try { result = JSON.parse(resultJSON); } catch (_) {}

        output({
            success: true,
            txId:    result.txId || 'TX_' + Date.now(),
            batchId,
            isCompliant: result.isCompliant,
            complianceAlerts: result.complianceAlerts || []
        });

    } catch (err) {
        // Fallback to CLI invocation via WSL if Gateway gRPC endorsement discovery has hostname resolution limits
        try {
            const fallbackResult = invokeViaCLI(batchId, sensorDataJSON);
            output(fallbackResult);
        } catch (cliErr) {
            output({ error: err.message || String(err) });
            process.exit(1);
        }
    } finally {
        if (gateway) gateway.close();
        if (client)  client.close();
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────
function invokeViaCLI(batchId, sensorDataJSON) {
    let cleanJSON = sensorDataJSON;
    try {
        const obj = typeof sensorDataJSON === 'string' ? JSON.parse(sensorDataJSON) : sensorDataJSON;
        cleanJSON = JSON.stringify(obj);
    } catch (_) {}

    const ctorJSON = JSON.stringify({
        Args: ['RecordSensorData', batchId, cleanJSON]
    });
    const ctorPath = path.resolve(__dirname, 'temp_ctor.json');
    fs.writeFileSync(ctorPath, ctorJSON, 'utf8');

    const scriptPath = '/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/test_invoke.sh';
    const wslCmd = `wsl -d Ubuntu bash "${scriptPath}"`;
    const stdout = execSync(wslCmd, { encoding: 'utf8' });
    const txIdMatch = stdout.match(/txId\\":\\"([a-f0-9]{64})\\"/);
    const txId = txIdMatch ? txIdMatch[1] : ('TX_' + Date.now());
    return {
        success: true,
        txId: txId,
        batchId: batchId,
        isCompliant: !stdout.includes('"isCompliant":false'),
        complianceAlerts: []
    };
}

async function newGrpcConnection() {
    const tlsRootCert   = fs.readFileSync(TLS_CERT_PATH);
    const tlsCredentials = grpc.credentials.createSsl(tlsRootCert);
    return new grpc.Client(PEER_ENDPOINT, tlsCredentials, {
        'grpc.ssl_target_name_override': PEER_HOST_ALIAS,
    });
}

async function newIdentity() {
    const credentials = fs.readFileSync(getCertPath());
    return { mspId: MSP_ID, credentials };
}

async function newSigner() {
    const keyFiles   = fs.readdirSync(KEY_DIR_PATH);
    const keyPath    = path.resolve(KEY_DIR_PATH, keyFiles[0]);
    const privateKey = crypto.createPrivateKey(fs.readFileSync(keyPath));
    return signers.newPrivateKeySigner(privateKey);
}

function output(obj) {
    process.stdout.write(JSON.stringify(obj) + '\n');
}

main().catch(err => {
    output({ error: err.message || String(err) });
    process.exit(1);
});
