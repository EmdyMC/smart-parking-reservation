#!/usr/bin/env bash
# Walkthrough demo: seed -> list -> lock race -> checkout -> (pay) -> confirmed
API=$(aws cloudformation describe-stacks --stack-name smart-parking \
  --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text)
step() { echo; echo "=== $* ==="; read -rp "(enter) "; }

step "1. Seed parking slots into DynamoDB"
curl -s -X POST "$API/seed"; echo

step "2. List slots (status from DynamoDB, live lock state from ElastiCache)"
curl -s "$API/slots" | python -m json.tool

step "3. Two users race for slot A1 at the same time"
curl -s -X POST "$API/reserve" -H 'content-type: application/json' -d '{"slotId":"A1","userId":"alice"}' > /tmp/alice.json &
curl -s -X POST "$API/reserve" -H 'content-type: application/json' -d '{"slotId":"A1","userId":"bob"}'   > /tmp/bob.json &
wait
echo "alice:"; python -m json.tool /tmp/alice.json
echo "bob:";   python -m json.tool /tmp/bob.json

step "4. Slot A1 now shows locked=true with a TTL"
curl -s "$API/slots" | python -c "import json,sys; print([s for s in json.load(sys.stdin) if s['slotId']=='A1'])"

RID=$(python -c "import json; d=json.load(open('/tmp/alice.json')); d=d if 'reservationId' in d else json.load(open('/tmp/bob.json')); print(d['reservationId']); print(d['checkoutUrl'], file=open('/tmp/url','w'))")
step "5. Open the Stripe checkout URL and pay with 4242 4242 4242 4242"
cat /tmp/url

step "6. After paying: the webhook confirmed the reservation and booked the slot"
curl -s "$API/reservations/$RID" | python -m json.tool
curl -s "$API/slots" | python -c "import json,sys; print([s for s in json.load(sys.stdin) if s['slotId']=='A1'])"
