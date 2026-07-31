#!/bin/bash
echo ""
echo " ============================================"
echo "  HYPERLEDGER FABRIC - FoodChain Network"
echo " ============================================"
echo ""
cd "/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/fabric-samples/test-network" || exit 1
echo "[FABRIC] Stopping any old network containers..."
./network.sh down
echo ""
echo "[FABRIC] Starting network + channel..."
./network.sh up createChannel -c mychannel
echo ""
echo "[FABRIC] Deploying FoodChain Chaincode..."
./network.sh deployCC -ccn foodchain -ccp ../../chaincode/foodchain -ccl javascript
echo ""
echo "[FABRIC] ✓ Network Ready! Peers running on port 7051"
echo "[FABRIC] ✓ Chaincode deployed: foodchain on mychannel"
echo ""
