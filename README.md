# Smart Parking Space Reservation Platform

**Architecture:** API Gateway (HTTP API) → Lambda (`parking-api`, Python 3.12) → DynamoDB (`parking-slots`, `parking-reservations`) + ElastiCache Serverless Valkey (`parking-locks`) → Stripe Checkout + Stripe webhook.

Everything lives in the CloudFormation stack `smart-parking` (ap-south-1) inside its own VPC (`10.42.0.0/16`). It shares nothing with any existing EC2 instance or VPC.

## Flow
1. `POST /reserve` → `SET lock:slot:<id> <reservationId> NX EX 1920` in ElastiCache. Only one caller can win the lock (409 for everyone else). A `PENDING_PAYMENT` reservation goes into DynamoDB and a Stripe Checkout Session is created. The response carries `checkoutUrl`.
2. The user pays on Stripe.
3. Stripe calls `POST /webhook`. The handler verifies the HMAC signature and handles two events:
   - `checkout.session.completed`: reservation goes to `CONFIRMED`, the slot to `BOOKED`, and the lock is released. A conditional write makes this idempotent.
   - `checkout.session.expired`: reservation goes to `EXPIRED` and the lock is released.
4. Locks auto-expire through their TTL if the webhook never arrives.

## Payment modes
- **Mock gateway (default, used for the demo).** If `StripeSecretKey` is empty, `/reserve` returns a `checkoutUrl` that points to a simulated hosted checkout page served by the Lambda. The page is clearly labelled as simulated and moves no money. Clicking **Pay** makes the gateway send a Stripe-format `checkout.session.completed` event, HMAC-signed the same way Stripe signs it (`Stripe-Signature: t=..,v1=..`), to the public `/webhook` URL. That means the real webhook path runs: signature check, idempotent confirmation, slot booking and lock release. **Cancel** sends `checkout.session.expired`. Card `4000 0000 0000 0002` simulates a decline (the slot stays locked so the user can retry).
- **Real Stripe.** Deploy with `StripeSecretKey`/`StripeWebhookSecret` (see below) and the same code calls the Stripe Checkout API instead. The mock routes are then disabled.

## Endpoints
| Method | Path | Body |
|---|---|---|
| POST | /seed | – (creates demo slots A1–A5, B1–B5) |
| GET | /slots | – (includes live `locked` / `lockTtl` from cache) |
| POST | /reserve | `{"slotId":"A1","userId":"u1"}` |
| POST | /release | `{"reservationId":"..."}` |
| GET | /reservations/{id} | – |
| POST | /webhook | Stripe event (signed) |
| GET | /mock-checkout/{reservationId} | – (mock mode: hosted checkout page) |
| POST | /mock-checkout/{reservationId}/pay \| /cancel | `{"card":"4242 4242 4242 4242"}` |

## Test
```bash
API=$(aws cloudformation describe-stacks --stack-name smart-parking --query "Stacks[0].Outputs[?OutputKey=='ApiUrl'].OutputValue" --output text)
curl -X POST $API/seed
curl $API/slots
curl -X POST $API/reserve -H 'content-type: application/json' -d '{"slotId":"A1","userId":"u1"}'
curl -X POST $API/reserve -H 'content-type: application/json' -d '{"slotId":"A1","userId":"u2"}'   # -> 409 locked
```
Open the returned `checkoutUrl` in a browser and pay with `4242 4242 4242 4242`. Or run `bash demo.sh` for a guided walkthrough.

## Configure real Stripe (optional)
1. In the Stripe test dashboard, go to Developers → Webhooks → Add endpoint. Set the URL to the `WebhookUrl` stack output and select the events `checkout.session.completed` and `checkout.session.expired`.
2. Run:
```bash
aws cloudformation deploy --template-file template.yaml --stack-name smart-parking --capabilities CAPABILITY_IAM \
  --parameter-overrides StripeSecretKey=sk_test_xxx StripeWebhookSecret=whsec_xxx
```
Code updates: `aws lambda update-function-code --function-name parking-api --zip-file fileb://app.zip`

## Teardown
`aws cloudformation delete-stack --stack-name smart-parking` (the NAT gateway costs about $0.05/hr, so delete the stack after grading).
