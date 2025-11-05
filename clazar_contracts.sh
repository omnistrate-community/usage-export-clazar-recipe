#!/usr/bin/env bash
#
# Clazar Contracts Fetcher
#
# Description:
#   This script authenticates with the Clazar API and prints a table of
#   contract IDs and buyer details (name, email, cloud, domain).
#
# Prerequisites:
#   1. curl  — for making API requests
#   2. jq    — for parsing JSON (install with: `sudo apt install jq` or `brew install jq`)
#
# Usage:
#   1. Make the script executable:
#        chmod +x clazar_contracts.sh
#   2. Run it:
#        ./clazar_contracts.sh
#   3. (Optional) Filter output using grep:
#        ./clazar_contracts.sh | grep "scalar"
#
# Notes:
#   - Update CLIENT_ID and CLIENT_SECRET below with your own credentials.
#   - Output is formatted as a readable table.
#

# --- Configuration ---
CLIENT_ID="YOUR_CLIENT_ID"
CLIENT_SECRET="YOUR_CLIENT_SECRET"
API_BASE="https://api.clazar.io"

# --- Authenticate and get access token ---
echo "Authenticating..."
TOKEN_RESPONSE=$(curl -s -X POST "$API_BASE/authenticate/" \
  -H "Content-Type: application/json" \
  -d "{\"client_id\": \"$CLIENT_ID\", \"client_secret\": \"$CLIENT_SECRET\"}")

ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')

if [[ "$ACCESS_TOKEN" == "null" || -z "$ACCESS_TOKEN" ]]; then
  echo "Error: Failed to obtain access token."
  echo "$TOKEN_RESPONSE"
  exit 1
fi

echo "Access token acquired."

# --- Get contracts and buyers ---
echo "Fetching data..."
CONTRACTS=$(curl -s -X GET "$API_BASE/contracts/" \
  -H "Authorization: Bearer $ACCESS_TOKEN")

BUYERS=$(curl -s -X GET "$API_BASE/buyers/" \
  -H "Authorization: Bearer $ACCESS_TOKEN")

# --- Print table header ---
printf "\n%-38s  %-20s  %-30s  %-8s  %-20s\n" "Contract ID" "Buyer Name" "Email" "Cloud" "Domain"
printf "%0.s-" {1..120}
echo

# --- Combine and print data as rows ---
echo "$CONTRACTS" | jq -r '.results[] | .id + " " + .buyer_id' | while read -r CONTRACT_ID BUYER_ID; do
  BUYER=$(echo "$BUYERS" | jq -r --arg id "$BUYER_ID" '.results[] | select(.id == $id)')
  NAME=$(echo "$BUYER" | jq -r '.name')
  EMAIL=$(echo "$BUYER" | jq -r '.registration_details[] | select(.field=="Email Address") | .value')
  CLOUD=$(echo "$BUYER" | jq -r '.cloud')
  DOMAIN=$(echo "$BUYER" | jq -r '.domain')

  printf "%-38s  %-20s  %-30s  %-8s  %-20s\n" "$CONTRACT_ID" "$NAME" "$EMAIL" "$CLOUD" "$DOMAIN"
done

echo
