'use strict';

/**
 * FoodChain Fabric Gateway - Transaction Submitter
 *
 * Usage:
 *   node submit_transaction.js <batchId> <sensorDataJSON>
 *
 * Output:
 *   {
 *     success: true,
 *     txId: "...",
 *     batchId: "...",
 *     isCompliant: true,
 *     complianceAlerts: []
 *   }
 */

const { connect, signers } = require('@hyperledger/fabric-gateway');
const grpc = require('@grpc/grpc-js');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

// ─────────────────────────────────────────────────────────────────────────────
// Fabric configuration
// ─────────────────────────────────────────────────────────────────────────────

const CHANNEL_NAME = 'foodchainchannel';
const CHAINCODE_NAME = 'foodchain';

const MSP_ID = 'Org1MSP';

const PEER_ENDPOINT = 'localhost:7051';
const PEER_HOST_ALIAS = 'peer0.org1.example.com';

// ─────────────────────────────────────────────────────────────────────────────
// Fabric test-network paths
// ─────────────────────────────────────────────────────────────────────────────

const TEST_NETWORK_PATH = path.resolve(
    __dirname,
    'fabric-samples',
    'test-network'
);

const CRYPTO_PATH = path.resolve(
    TEST_NETWORK_PATH,
    'organizations',
    'peerOrganizations',
    'org1.example.com'
);

const USER_MSP_PATH = path.resolve(
    CRYPTO_PATH,
    'users',
    'User1@org1.example.com',
    'msp'
);

const KEY_DIR_PATH = path.resolve(
    USER_MSP_PATH,
    'keystore'
);

const CERT_DIR_PATH = path.resolve(
    USER_MSP_PATH,
    'signcerts'
);

const TLS_CERT_PATH = path.resolve(
    CRYPTO_PATH,
    'peers',
    'peer0.org1.example.com',
    'tls',
    'ca.crt'
);

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function output(obj) {
    process.stdout.write(JSON.stringify(obj) + '\n');
}

function fail(message) {
    output({
        success: false,
        error: message
    });

    process.exit(1);
}

function getCertPath() {
    if (!fs.existsSync(CERT_DIR_PATH)) {
        throw new Error(
            `Fabric identity certificate directory not found: ${CERT_DIR_PATH}`
        );
    }

    const files = fs
        .readdirSync(CERT_DIR_PATH)
        .filter(file => file.endsWith('.pem'));

    if (files.length === 0) {
        throw new Error(
            `No certificate found in: ${CERT_DIR_PATH}`
        );
    }

    return path.join(CERT_DIR_PATH, files[0]);
}

function getKeyPath() {
    if (!fs.existsSync(KEY_DIR_PATH)) {
        throw new Error(
            `Fabric private-key directory not found: ${KEY_DIR_PATH}`
        );
    }

    const files = fs
        .readdirSync(KEY_DIR_PATH)
        .filter(file => file.endsWith('_sk'));

    if (files.length === 0) {
        throw new Error(
            `No private key found in: ${KEY_DIR_PATH}`
        );
    }

    return path.join(KEY_DIR_PATH, files[0]);
}

// ─────────────────────────────────────────────────────────────────────────────
// gRPC connection
// ─────────────────────────────────────────────────────────────────────────────

async function newGrpcConnection() {
    if (!fs.existsSync(TLS_CERT_PATH)) {
        throw new Error(
            `Fabric TLS certificate not found: ${TLS_CERT_PATH}`
        );
    }

    const tlsRootCert = fs.readFileSync(TLS_CERT_PATH);

    const tlsCredentials = grpc.credentials.createSsl(
        tlsRootCert
    );

    return new grpc.Client(
        PEER_ENDPOINT,
        tlsCredentials,
        {
            'grpc.ssl_target_name_override': PEER_HOST_ALIAS
        }
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Fabric identity
// ─────────────────────────────────────────────────────────────────────────────

async function newIdentity() {
    const certPath = getCertPath();

    const credentials = fs.readFileSync(certPath);

    return {
        mspId: MSP_ID,
        credentials
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Fabric signer
// ─────────────────────────────────────────────────────────────────────────────

async function newSigner() {
    const keyPath = getKeyPath();

    const privateKey = crypto.createPrivateKey(
        fs.readFileSync(keyPath)
    );

    return signers.newPrivateKeySigner(privateKey);
}

// ─────────────────────────────────────────────────────────────────────────────
// Main transaction function
// ─────────────────────────────────────────────────────────────────────────────

async function main() {

    const args = process.argv.slice(2);

    const batchId = args[0];
    const sensorDataJSON = args[1];

    if (!batchId || !sensorDataJSON) {
        fail(
            'Usage: node submit_transaction.js <batchId> <sensorDataJSON>'
        );
    }

    // Validate JSON before contacting Fabric.
    let sensorData;

    try {
        sensorData = JSON.parse(sensorDataJSON);
    } catch (err) {
        fail(`Invalid sensorDataJSON: ${err.message}`);
    }

    // Re-serialize to ensure clean JSON is sent to chaincode.
    const cleanSensorDataJSON = JSON.stringify(sensorData);

    let client = null;
    let gateway = null;

    try {

        console.error(
            `[FABRIC] Connecting to ${PEER_ENDPOINT}...`
        );

        client = await newGrpcConnection();

        gateway = connect({
            client,

            identity: await newIdentity(),

            signer: await newSigner(),

            evaluateOptions: () => ({
                deadline: Date.now() + 10000
            }),

            endorseOptions: () => ({
                deadline: Date.now() + 30000
            }),

            submitOptions: () => ({
                deadline: Date.now() + 30000
            }),

            commitStatusOptions: () => ({
                deadline: Date.now() + 60000
            })
        });

        console.error(
            `[FABRIC] Connected. Channel=${CHANNEL_NAME}, Chaincode=${CHAINCODE_NAME}`
        );

        const network = gateway.getNetwork(
            CHANNEL_NAME
        );

        const contract = network.getContract(
            CHAINCODE_NAME
        );

        console.error(
            `[FABRIC] Submitting RecordSensorData for batch ${batchId}...`
        );

        // ─────────────────────────────────────────────────────────────
        // Create proposal
        // ─────────────────────────────────────────────────────────────

        const proposal = contract.newProposal(
            'RecordSensorData',
            {
                arguments: [
                    batchId,
                    cleanSensorDataJSON
                ]
            }
        );

        // ─────────────────────────────────────────────────────────────
        // Endorse transaction
        // ─────────────────────────────────────────────────────────────

        const transaction = await proposal.endorse();

        // This is the REAL Fabric transaction ID.
        const fabricTxId = transaction.getTransactionId();

        if (!fabricTxId) {
            throw new Error(
                'Fabric Gateway returned no transaction ID after endorsement'
            );
        }

        console.error(
            `[FABRIC] Transaction ID: ${fabricTxId}`
        );

        // ─────────────────────────────────────────────────────────────
        // Submit transaction to ordering service
        // ─────────────────────────────────────────────────────────────

        const submittedTransaction =
            await transaction.submit();

        // ─────────────────────────────────────────────────────────────
        // Read chaincode result
        // ─────────────────────────────────────────────────────────────

        let result = {};

        try {

            const resultBytes =
                submittedTransaction.getResult();

            if (resultBytes) {

                const resultJSON =
                    Buffer.from(resultBytes).toString('utf8');

                if (resultJSON.trim()) {
                    result = JSON.parse(resultJSON);
                }
            }

        } catch (err) {

            console.error(
                `[FABRIC] Could not parse chaincode result: ${err.message}`
            );
        }

        // ─────────────────────────────────────────────────────────────
        // Wait for ledger commit
        // ─────────────────────────────────────────────────────────────

        const commitStatus =
            await submittedTransaction.getStatus();

        if (!commitStatus.successful) {

            throw new Error(
                `Fabric transaction ${commitStatus.transactionId || fabricTxId} failed with status code ${commitStatus.code}`
            );
        }

        console.error(
            `[FABRIC] Transaction committed successfully: ${fabricTxId}`
        );

        // ─────────────────────────────────────────────────────────────
        // Return clean JSON to FastAPI
        // ─────────────────────────────────────────────────────────────

        output({
            success: true,

            txId: fabricTxId,

            batchId: batchId,

            isCompliant:
                result.isCompliant !== undefined
                    ? result.isCompliant
                    : true,

            complianceAlerts:
                Array.isArray(result.complianceAlerts)
                    ? result.complianceAlerts
                    : []
        });

    } catch (err) {

        console.error(
            '[FABRIC GATEWAY ERROR]'
        );

        console.error(
            err.stack || err.message || String(err)
        );

        output({
            success: false,

            error:
                err.message ||
                String(err),

            batchId: batchId,

            gatewayError: true
        });

        process.exitCode = 1;

    } finally {

        if (gateway) {
            try {
                gateway.close();
            } catch (_) {}
        }

        if (client) {
            try {
                client.close();
            } catch (_) {}
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Start
// ─────────────────────────────────────────────────────────────────────────────

main().catch(err => {

    output({
        success: false,
        error: err.message || String(err)
    });

    process.exit(1);
});