#!/bin/bash
# ==========================================
# EventFlow Load Test Script
# ==========================================
#
# WHAT IS LOAD TESTING?
# Load testing sends a large number of requests to your system
# to see how it performs under pressure. It answers:
#   - How many events/second can we handle?
#   - What's the latency at high throughput?
#   - Where does the system break?
#
# USAGE:
#   ./scripts/load-test.sh              # Default: 1000 events
#   ./scripts/load-test.sh 10000        # Custom: 10000 events
#   ./scripts/load-test.sh 10000 50     # 10000 events, 50 concurrent
#
# REQUIRES: curl (installed on all Mac/Linux systems)

set -e  # Exit on any error

# Configuration
BASE_URL="${BASE_URL:-http://localhost:8080}"
TOTAL_EVENTS="${1:-1000}"
CONCURRENCY="${2:-10}"
API_KEY="load-test-key"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  EventFlow Load Test${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Target:      ${BASE_URL}"
echo "Events:      ${TOTAL_EVENTS}"
echo "Concurrency: ${CONCURRENCY}"
echo ""

# Check if the service is running
echo -n "Checking service health... "
HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/health" 2>/dev/null)
if [ "$HEALTH" != "200" ]; then
    echo -e "${RED}FAILED${NC}"
    echo "Service is not running at ${BASE_URL}"
    echo "Start it with: docker-compose up -d"
    exit 1
fi
echo -e "${GREEN}OK${NC}"
echo ""

# Event types to randomly pick from
EVENT_TYPES=("page_view" "click" "purchase" "search" "login" "scroll" "add_to_cart" "signup" "error" "logout")
PAGES=("/home" "/products" "/cart" "/checkout" "/profile" "/settings" "/search" "/about")

# Function to generate a random event JSON
generate_event() {
    local user_num=$((RANDOM % 100))  # 100 different users
    local event_idx=$((RANDOM % ${#EVENT_TYPES[@]}))
    local page_idx=$((RANDOM % ${#PAGES[@]}))
    local duration=$((RANDOM % 300))

    cat <<EOF
{
    "eventType": "${EVENT_TYPES[$event_idx]}",
    "userId": "user-$(printf '%03d' $user_num)",
    "payload": {
        "page": "${PAGES[$page_idx]}",
        "duration": ${duration},
        "sessionId": "sess-$(date +%s)-${RANDOM}",
        "browser": "Chrome",
        "platform": "web"
    }
}
EOF
}

# Counters
SUCCESS=0
FAILED=0
TOTAL_TIME=0

# Start time
START=$(date +%s%N)

echo -e "${YELLOW}Sending ${TOTAL_EVENTS} events...${NC}"
echo ""

# Send events with controlled concurrency
for i in $(seq 1 $TOTAL_EVENTS); do
    EVENT_JSON=$(generate_event)

    # Send in background for concurrency
    (
        RESPONSE=$(curl -s -o /dev/null -w "%{http_code}:%{time_total}" \
            -X POST "${BASE_URL}/api/v1/events" \
            -H "Content-Type: application/json" \
            -H "X-API-Key: ${API_KEY}" \
            -d "${EVENT_JSON}" 2>/dev/null)

        HTTP_CODE=$(echo $RESPONSE | cut -d: -f1)
        TIME=$(echo $RESPONSE | cut -d: -f2)

        if [ "$HTTP_CODE" = "202" ]; then
            echo "OK"
        elif [ "$HTTP_CODE" = "429" ]; then
            echo "RATE_LIMITED"
        else
            echo "FAILED:${HTTP_CODE}"
        fi
    ) &

    # Control concurrency: wait if we have too many background jobs
    if [ $(( i % CONCURRENCY )) -eq 0 ]; then
        wait
    fi

    # Progress indicator
    if [ $(( i % 100 )) -eq 0 ]; then
        echo -e "  Sent ${i}/${TOTAL_EVENTS} events..."
    fi
done

# Wait for all remaining background jobs
wait

# End time
END=$(date +%s%N)
ELAPSED_MS=$(( (END - START) / 1000000 ))
ELAPSED_SEC=$(echo "scale=2; $ELAPSED_MS / 1000" | bc)

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Load Test Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Duration:         ${ELAPSED_SEC} seconds"
echo "Total events:     ${TOTAL_EVENTS}"
echo "Throughput:       $(echo "scale=0; $TOTAL_EVENTS * 1000 / $ELAPSED_MS" | bc) events/sec"
echo ""

# Query some results
echo -e "${YELLOW}Verifying data in Cassandra...${NC}"
echo ""

# Check a random user's events
SAMPLE_USER="user-042"
STORED=$(curl -s "${BASE_URL}/api/v1/events?userId=${SAMPLE_USER}&hours=1" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "N/A")
echo "Events stored for ${SAMPLE_USER}: ${STORED}"

# Check anomalies
ANOMALIES=$(curl -s "${BASE_URL}/api/v1/events/anomalies?hours=1" | python3 -c "import sys,json; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "N/A")
echo "Anomalies detected: ${ANOMALIES}"

echo ""
echo -e "${GREEN}Dashboard: http://localhost:3000 (admin/admin)${NC}"
echo -e "${GREEN}Prometheus: http://localhost:9090${NC}"
echo ""
echo "Try these Prometheus queries:"
echo "  rate(eventflow_events_ingested_total[1m])"
echo "  eventflow_ingestion_latency_seconds{quantile=\"0.99\"}"
echo "  eventflow_events_failed_total"
