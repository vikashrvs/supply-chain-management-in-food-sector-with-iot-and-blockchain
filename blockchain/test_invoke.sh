#!/bin/bash
cd "/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/fabric-samples/test-network" || exit 1
export PATH=$PATH:$(pwd)/../bin
export FABRIC_CFG_PATH=$(pwd)/../config
. scripts/envVar.sh
setGlobals 1

CTOR_FILE="/mnt/c/Users/raj vikash/Desktop/food_chain/blockchain/temp_ctor.json"

peer chaincode invoke \
  -o localhost:7050 \
  --ordererTLSHostnameOverride orderer.example.com \
  --tls \
  --cafile "$(pwd)/organizations/ordererOrganizations/example.com/tlsca/tlsca.example.com-cert.pem" \
  -C mychannel \
  -n foodchain \
  --peerAddresses localhost:7051 \
  --tlsRootCertFiles "$(pwd)/organizations/peerOrganizations/org1.example.com/tlsca/tlsca.org1.example.com-cert.pem" \
  --peerAddresses localhost:9051 \
  --tlsRootCertFiles "$(pwd)/organizations/peerOrganizations/org2.example.com/tlsca/tlsca.org2.example.com-cert.pem" \
  -c "$(cat "${CTOR_FILE}")"
