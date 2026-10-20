"""Smart Parking Reservation API (single Lambda behind HTTP API Gateway).

Routes:
  POST /seed                      create demo slots in DynamoDB
  GET  /slots                     list slots with live lock state from ElastiCache
  POST /reserve  {slotId,userId}  lock slot in ElastiCache (SET NX EX), create PENDING
                                  reservation in DynamoDB, open Stripe Checkout session
  POST /release  {reservationId}  cancel a pending reservation and free the lock
  GET  /reservations/{id}         reservation status
  POST /webhook                   Stripe webhook (signature verified) -> confirm/expire
"""
import base64, hashlib, hmac, json, os, socket, ssl, time, uuid
import urllib.parse, urllib.request

import boto3
from boto3.dynamodb.conditions import Attr

ddb = boto3.resource("dynamodb")
slots = ddb.Table(os.environ["SLOTS_TABLE"])
reservations = ddb.Table(os.environ["RES_TABLE"])
STRIPE_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
# Stripe Checkout sessions live at least 30 min; hold the lock a bit longer than that.
SESSION_TTL = 1800
LOCK_TTL = SESSION_TTL + 120


# ---------- Minimal RESP client over TLS (ElastiCache serverless requires TLS) ----------
class Redis:
    def __init__(self, host, port):
        self.host, self.port, self.sock, self.buf = host, int(port), None, b""

    def _connect(self):
        raw = socket.create_connection((self.host, self.port), timeout=3)
        self.sock = ssl.create_default_context().wrap_socket(raw, server_hostname=self.host)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise ConnectionError("cache closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _read(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.sock.recv(4096)
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data.decode()
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise RuntimeError(f"bad reply {line!r}")

    def cmd(self, *args):
        payload = f"*{len(args)}\r\n".encode()
        for a in args:
            b = str(a).encode()
            payload += b"$%d\r\n%s\r\n" % (len(b), b)
        for attempt in (0, 1):
            try:
                if self.sock is None:
                    self._connect()
                self.sock.sendall(payload)
                return self._read()
            except (OSError, ConnectionError):
                self.sock = None
                if attempt:
                    raise


cache = Redis(os.environ["CACHE_HOST"], os.environ["CACHE_PORT"])

# Release the lock only if it is still held by this reservation (atomic compare-and-delete).
RELEASE_LUA = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"


def lock_key(slot_id):
    return f"lock:slot:{slot_id}"


def release_lock(slot_id, reservation_id):
    return cache.cmd("EVAL", RELEASE_LUA, 1, lock_key(slot_id), reservation_id)


# ---------- Stripe ----------
def stripe_checkout(reservation_id, slot, base_url):
    form = {
        "mode": "payment",
        "success_url": f"{base_url}/reservations/{reservation_id}?paid=1",
        "cancel_url": f"{base_url}/reservations/{reservation_id}?cancelled=1",
        "client_reference_id": reservation_id,
        "expires_at": int(time.time()) + SESSION_TTL,
        "metadata[reservationId]": reservation_id,
        "metadata[slotId]": slot["slotId"],
        "line_items[0][quantity]": 1,
        "line_items[0][price_data][currency]": "inr",
        "line_items[0][price_data][unit_amount]": int(slot["priceInr"]) * 100,
        "line_items[0][price_data][product_data][name]": f"Parking slot {slot['slotId']} ({slot.get('zone', '')})",
    }
    req = urllib.request.Request(
        "https://api.stripe.com/v1/checkout/sessions",
        data=urllib.parse.urlencode(form).encode(),
        headers={"Authorization": f"Bearer {STRIPE_KEY}", "Idempotency-Key": reservation_id},
    )
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read())


def verify_stripe_signature(body, header, tolerance=300):
    parts = {}
    for item in header.split(","):
        k, _, v = item.partition("=")
        parts.setdefault(k, []).append(v)
    ts = parts.get("t", ["0"])[0]
    if abs(time.time() - int(ts)) > tolerance:
        return False
    expected = hmac.new(WEBHOOK_SECRET.encode(), f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()
    return any(hmac.compare_digest(expected, sig) for sig in parts.get("v1", []))


# ---------- Handlers ----------
def resp(code, body):
    return {"statusCode": code, "headers": {"Content-Type": "application/json"}, "body": json.dumps(body, default=str)}


def seed(_event):
    with slots.batch_writer() as w:
        for zone, prefix in (("Level-1", "A"), ("Level-2", "B")):
            for i in range(1, 6):
                w.put_item(Item={"slotId": f"{prefix}{i}", "zone": zone, "priceInr": 50 if prefix == "A" else 30, "status": "AVAILABLE"})
    return resp(200, {"seeded": 10})


def list_slots(_event):
    items = sorted(slots.scan()["Items"], key=lambda s: s["slotId"])
    for s in items:
        holder = cache.cmd("GET", lock_key(s["slotId"]))
        s["locked"] = holder is not None
        s["lockTtl"] = cache.cmd("TTL", lock_key(s["slotId"])) if holder else None
    return resp(200, items)


def reserve(event, base_url):
    body = json.loads(event.get("body") or "{}")
    slot_id, user_id = body.get("slotId"), body.get("userId")
    if not slot_id or not user_id:
        return resp(400, {"error": "slotId and userId required"})
    slot = slots.get_item(Key={"slotId": slot_id}).get("Item")
    if not slot:
        return resp(404, {"error": "slot not found"})
    if slot["status"] != "AVAILABLE":
        return resp(409, {"error": "slot already booked"})

    reservation_id = str(uuid.uuid4())
    # Real-time lock: only one caller can win SET NX for this slot.
    if cache.cmd("SET", lock_key(slot_id), reservation_id, "NX", "EX", LOCK_TTL) != "OK":
        return resp(409, {"error": "slot is currently locked by another checkout", "retryAfterSec": cache.cmd("TTL", lock_key(slot_id))})

    now = int(time.time())
    reservations.put_item(Item={
        "reservationId": reservation_id, "slotId": slot_id, "userId": user_id,
        "status": "PENDING_PAYMENT", "amountInr": slot["priceInr"], "createdAt": now,
        "lockExpiresAt": now + LOCK_TTL,
    })
    if not STRIPE_KEY:
        release_lock(slot_id, reservation_id)
        reservations.update_item(Key={"reservationId": reservation_id}, UpdateExpression="SET #s = :s",
                                 ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":s": "FAILED"})
        return resp(500, {"error": "STRIPE_SECRET_KEY not configured"})
    try:
        session = stripe_checkout(reservation_id, slot, base_url)
    except Exception as e:
        release_lock(slot_id, reservation_id)
        detail = e.read().decode() if hasattr(e, "read") else str(e)
        return resp(502, {"error": "stripe error", "detail": detail})

    reservations.update_item(Key={"reservationId": reservation_id}, UpdateExpression="SET checkoutSessionId = :c",
                             ExpressionAttributeValues={":c": session["id"]})
    return resp(201, {"reservationId": reservation_id, "slotId": slot_id, "lockTtlSec": LOCK_TTL,
                      "checkoutUrl": session["url"], "checkoutSessionId": session["id"]})


def release(event):
    rid = json.loads(event.get("body") or "{}").get("reservationId")
    r = reservations.get_item(Key={"reservationId": rid}).get("Item") if rid else None
    if not r:
        return resp(404, {"error": "reservation not found"})
    if r["status"] != "PENDING_PAYMENT":
        return resp(409, {"error": f"cannot release reservation in status {r['status']}"})
    set_status(rid, "CANCELLED", "PENDING_PAYMENT")
    release_lock(r["slotId"], rid)
    return resp(200, {"reservationId": rid, "status": "CANCELLED"})


def get_reservation(rid):
    r = reservations.get_item(Key={"reservationId": rid}).get("Item")
    return resp(200, r) if r else resp(404, {"error": "reservation not found"})


def set_status(rid, status, expected):
    try:
        reservations.update_item(
            Key={"reservationId": rid}, UpdateExpression="SET #s = :n, updatedAt = :t",
            ConditionExpression=Attr("status").eq(expected),
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":n": status, ":t": int(time.time())})
        return True
    except ddb.meta.client.exceptions.ConditionalCheckFailedException:
        return False


def webhook(event):
    body = event.get("body") or ""
    if event.get("isBase64Encoded"):
        body = base64.b64decode(body).decode()
    sig = (event.get("headers") or {}).get("stripe-signature", "")
    if not WEBHOOK_SECRET or not verify_stripe_signature(body, sig):
        return resp(400, {"error": "invalid signature"})

    evt = json.loads(body)
    obj = evt["data"]["object"]
    rid = (obj.get("metadata") or {}).get("reservationId")
    slot_id = (obj.get("metadata") or {}).get("slotId")
    if not rid:
        return resp(200, {"ignored": evt["type"]})

    if evt["type"] == "checkout.session.completed" and obj.get("payment_status") == "paid":
        # Idempotent: only the first delivery flips PENDING -> CONFIRMED.
        if set_status(rid, "CONFIRMED", "PENDING_PAYMENT"):
            slots.update_item(Key={"slotId": slot_id}, UpdateExpression="SET #s = :b, reservationId = :r",
                              ExpressionAttributeNames={"#s": "status"},
                              ExpressionAttributeValues={":b": "BOOKED", ":r": rid})
            reservations.update_item(Key={"reservationId": rid}, UpdateExpression="SET paymentIntent = :p",
                                     ExpressionAttributeValues={":p": obj.get("payment_intent")})
        release_lock(slot_id, rid)
    elif evt["type"] == "checkout.session.expired":
        set_status(rid, "EXPIRED", "PENDING_PAYMENT")
        release_lock(slot_id, rid)
    return resp(200, {"received": evt["type"]})


def handler(event, _context):
    method = event["requestContext"]["http"]["method"]
    path = event.get("rawPath", "/")
    base_url = f"https://{event['requestContext']['domainName']}"
    try:
        if method == "POST" and path == "/seed":
            return seed(event)
        if method == "GET" and path == "/slots":
            return list_slots(event)
        if method == "POST" and path == "/reserve":
            return reserve(event, base_url)
        if method == "POST" and path == "/release":
            return release(event)
        if method == "GET" and path.startswith("/reservations/"):
            return get_reservation(path.rsplit("/", 1)[1])
        if method == "POST" and path == "/webhook":
            return webhook(event)
        return resp(404, {"error": f"no route {method} {path}"})
    except Exception as e:
        print("ERROR", repr(e))
        return resp(500, {"error": str(e)})
