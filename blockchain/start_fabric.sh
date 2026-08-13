#!/bin/bash

# FoodChain Hyperledger Fabric Network Startup Script
# Starts Fabric, creates foodchainchannel, and deploys FoodChain chaincode.

set -e

NETWORK_DIR="/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/fabric-samples/test-network"
CHAINCODE_PATH="../../chaincode/foodchain"
CHANNEL_NAME="foodchainchannel"
CHAINCODE_NAME="foodchain"

echo ""
echo " ============================================"
echo "  HYPERLEDGER FABRIC - FoodChain Network"
echo " ============================================"
echo ""

cd "$NETWORK_DIR" || {
    echo "[ERROR] test-network directory not found!"
    exit 1
}

echo "[FABRIC] Stopping any old network containers..."
./network.sh down 2>&1 || true
sleep 2

echo ""
echo "[FABRIC] Starting Fabric network..."
./network.sh up -ca

echo ""
echo "[FABRIC] Creating channel: $CHANNEL_NAME"
./network.sh createChannel -c "$CHANNEL_NAME"

echo ""
echo "[FABRIC] Waiting for peers..."
sleep 5

echo ""
echo "[FABRIC] Deploying FoodChain chaincode..."
./network.sh deployCC \
    -ccn "$CHAINCODE_NAME" \
    -ccp "$CHAINCODE_PATH" \
    -ccl javascript \
    -c "$CHANNEL_NAME"

echo ""
echo "[FABRIC] Waiting for chaincode..."
sleep 5

echo ""
echo " ============================================"
echo "  FoodChain Fabric Network READY"
echo " ============================================"
echo "  Channel:    $CHANNEL_NAME"
echo "  Chaincode:  $CHAINCODE_NAME"
echo "  Peer Org1:  localhost:7051"
echo "  Peer Org2:  localhost:9051"
echo "  Orderer:    localhost:7050"
echo " ============================================"
echo ""
echo "[FABRIC] Backend can now connect to Fabric."
echo ""