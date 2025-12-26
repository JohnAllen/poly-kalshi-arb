#!/bin/bash
# Approve Polymarket CTF Exchange to sell conditional tokens
# This is required for SELL orders to work

set -e

# Contract addresses on Polygon
CTF_CONTRACT="0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"  # Conditional Token Framework
CTF_EXCHANGE="0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"  # CTF Exchange (non-neg-risk)
NEG_RISK_EXCHANGE="0xC5d563A36AE78145C45a50134d48A1215220f80a"  # Neg Risk CTF Exchange
NEG_RISK_ADAPTER="0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"  # Neg Risk Adapter

# Load environment
if [ -f .env ]; then
    source .env
elif [ -f ~/poly-kalshi-arb/.env ]; then
    source ~/poly-kalshi-arb/.env
fi

if [ -z "$POLY_PRIVATE_KEY" ]; then
    echo "Error: POLY_PRIVATE_KEY not set"
    exit 1
fi

RPC_URL="${POLYGON_RPC:-https://polygon-rpc.com}"

echo "Approving CTF Exchange for conditional token transfers..."
echo "CTF Contract: $CTF_CONTRACT"
echo "CTF Exchange: $CTF_EXCHANGE"
echo "Neg Risk Exchange: $NEG_RISK_EXCHANGE"
echo ""

# setApprovalForAll(address operator, bool approved)
# Function selector: 0xa22cb465

echo "1/3 Approving CTF Exchange..."
~/.foundry/bin/cast send "$CTF_CONTRACT" \
    "setApprovalForAll(address,bool)" \
    "$CTF_EXCHANGE" \
    true \
    --private-key "$POLY_PRIVATE_KEY" \
    --rpc-url "$RPC_URL"

echo ""
echo "2/3 Approving Neg Risk Exchange..."
~/.foundry/bin/cast send "$CTF_CONTRACT" \
    "setApprovalForAll(address,bool)" \
    "$NEG_RISK_EXCHANGE" \
    true \
    --private-key "$POLY_PRIVATE_KEY" \
    --rpc-url "$RPC_URL"

echo ""
echo "3/3 Approving Neg Risk Adapter..."
~/.foundry/bin/cast send "$CTF_CONTRACT" \
    "setApprovalForAll(address,bool)" \
    "$NEG_RISK_ADAPTER" \
    true \
    --private-key "$POLY_PRIVATE_KEY" \
    --rpc-url "$RPC_URL"

echo ""
echo "Done! Conditional token approvals are set."
echo "You can now sell positions on Polymarket."
