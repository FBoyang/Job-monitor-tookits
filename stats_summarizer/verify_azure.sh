#!/bin/bash
# Verification script for Azure OpenAI at HMS
# Run: ./verify_azure.sh  or  bash verify_azure.sh

set -e

ENDPOINT="${AZURE_OPENAI_ENDPOINT:-https://azure-ai.hms.edu}"
DEPLOYMENT="${AZURE_OPENAI_DEPLOYMENT:-gpt-5}"
API_VERSION="${AZURE_OPENAI_API_VERSION:-2025-03-01-preview}"

echo "=== Azure OpenAI verification ==="
echo "Endpoint: $ENDPOINT"
echo "Deployment: $DEPLOYMENT"
echo ""

# 1. Check env var
if [ -z "$AZURE_OPENAI_API_KEY" ]; then
    echo "FAIL: AZURE_OPENAI_API_KEY is not set"
    echo "  Run: export AZURE_OPENAI_API_KEY=your-key"
    exit 1
fi
echo "OK: AZURE_OPENAI_API_KEY is set (length: ${#AZURE_OPENAI_API_KEY})"

# 2. Test API call
echo ""
echo "Testing API call..."
RESP=$(curl -s -w "\n%{http_code}" -X POST \
  "${ENDPOINT}/openai/deployments/${DEPLOYMENT}/chat/completions?api-version=${API_VERSION}" \
  -H "Content-Type: application/json" \
  -H "api-key: ${AZURE_OPENAI_API_KEY}" \
  -d '{"messages": [{"role": "user", "content": "Say hello in one word"}]}')

HTTP_CODE=$(echo "$RESP" | tail -n1)
BODY=$(echo "$RESP" | sed '$d')

echo "HTTP status: $HTTP_CODE"
echo "Response: $BODY"
echo ""

if [ "$HTTP_CODE" = "200" ]; then
    echo "SUCCESS: API is working. You can run stats_summarizer."
elif [ "$HTTP_CODE" = "401" ]; then
    echo "FAIL: Invalid API key (401)"
    echo "  - Verify the key in Azure portal matches this endpoint"
    echo "  - Production: https://azure-ai.hms.edu"
    echo "  - Dev:        https://azure-ai-dev.hms.edu (may block cluster IPs)"
    echo "  - Key may be expired or rotated"
    exit 1
elif echo "$BODY" | grep -q Incapsula; then
    echo "FAIL: Request blocked by Incapsula (WAF)"
    echo "  - Cluster IP (140.247.x.x) may be blocked"
    echo "  - Try from laptop on campus WiFi or VPN"
    echo "  - Or use production: export AZURE_OPENAI_ENDPOINT=https://azure-ai.hms.edu"
    echo "  - Contact HMS IT to allow cluster IPs"
    exit 1
else
    echo "FAIL: Unexpected response (HTTP $HTTP_CODE)"
    exit 1
fi
