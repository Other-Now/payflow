# payflow — interview study sheet

Read this top to bottom once; it is ordered by how likely the question is.

## 30-second pitch

A payment orchestration backend in Java 21 / Spring Boot 3, shaped like a
travel checkout: a REST checkout API calls an **eligibility service over gRPC**
(which payment methods may this hotel/flight/car checkout offer), then runs a
**payment saga** across card / wallet / gift-card providers (mock, over gRPC),
with state in **DynamoDB**. The interesting part is correctness under failure:
retries, duplicate submits, provider timeouts and the service being `kill -9`ed
mid-payment. A chaos harness checks our database against the *provider's own
ledger* and must find 0 double charges and 0 discrepancies; the same harness
with idempotency switched off finds double charges, which is how I know the
check can fail.

## The five things to be able to draw

### 1. Where idempotency lives (two layers)

```
client --Idempotency-Key--> payment-service --pspKey = paymentId:leg--> provider
```

* **API layer.** `IDEM#{key}` item and the new `PAY#{id}` item are written in
  one `TransactWriteItems`, both `attribute_not_exists(pk)`. A concurrent
  duplicate loses the condition and gets the winner's payment back. Same key +
  different body (SHA-256 of the request) → 422. TTL 24 h on `expiresAt`.
* **Provider layer.** Each leg's provider key is *derived* (`paymentId:legIndex`),
  never random. So retries, a restarted process and the reconciler all address
  the same authorization. The provider dedups on it.
* The negative control (`unsafe-mode`) breaks exactly this: a new key per retry.

> "Why both?" The API key stops the *client* creating two payments; the derived
> provider key stops *us* creating two holds when we retry or crash.

### 2. The unknown outcome (the classic payments question)

A timeout does **not** mean the charge failed. `ProviderClient` classifies every
call:

| outcome | meaning | saga does |
|---|---|---|
| `Answered` | provider replied | trust the status |
| `Unknown` | ≥1 request left the process, no reply | leg → `UNKNOWN`, payment stays `AUTHORIZING`, API returns **202** |
| `NotSent` | circuit breaker refused before sending | safe to treat as a decline |

Subtle bit: if the breaker opens *between retries*, earlier attempts may have
landed, so that's `Unknown`, not `NotSent` (`sent` counter in `ProviderClient.call`).

The **reconciler** finds in-flight payments idle longer than `stale-after-ms`
(via a sparse GSI), asks the provider `GetStatus(pspKey)` (never faulted, not
behind the breaker), then: every leg authorized → roll forward to `AUTHORIZED`;
anything else → compensate.

**The race people miss:** reconciler sees `NOT_FOUND`, marks failed — and then
the slow original request lands and places a hold. Fix: compensation sends
`Void(key)` even for keys the provider has never seen; the mock writes a
**tombstone**, so any later `Authorize` with that key returns `VOIDED`. (Real
PSPs: cancel-by-reference, or auths auto-expire + settlement-file recon.)

### 3. Split tender = saga with compensation

Gift card is always authorized first (sorted in `PaymentService.newPayment`;
the test sends card first on purpose). Card declined → `COMPENSATING` is saved
**before** voiding anything → void every leg not provably declined → `FAILED`.
If the process dies mid-compensation, the reconciler finds `COMPENSATING` and
finishes it. Void is idempotent by state, so re-running is safe.

### 4. DynamoDB model (single table)

| pk | sk | holds |
|---|---|---|
| `PAY#{id}` | `PAYMENT` | payment + tenders list, `version` |
| `IDEM#{key}` | `IDEM` | paymentId, requestHash, `expiresAt` (TTL) |

GSI `inflight`: `gsi1pk = INFLIGHT#{0..3}`, `gsi1sk = updatedAt` (N), KEYS_ONLY.

* **One item per payment** so a state change (payment status + leg statuses) is
  one conditional write — no cross-item transaction needed on the hot path.
* **Optimistic locking**: every save is `PutItem ... ConditionExpression version = :v`.
  Capture-vs-void race → exactly one wins, the other gets 409. Reconciler vs a
  live request → the loser stops.
* **Sparse GSI**: only in-flight payments have `gsi1pk`, so the index is just
  the reconciler's work queue. **Sharded 4 ways** to avoid one hot partition key.
  **KEYS_ONLY + re-read** with `ConsistentRead` because GSIs are eventually consistent.
* Access patterns: get by id; get by idempotency key; create both atomically;
  versioned update; "stale in-flight older than T". No scans on the hot path
  (`/internal/payments` scan exists only for the audit).

### 5. Resilience (Resilience4j, explicit code, no annotations)

* gRPC **deadline** per attempt (`withDeadlineAfter`, 500–800 ms).
* **Retry** 3 attempts, exponential backoff with jitter, only on
  `UNAVAILABLE`/`DEADLINE_EXCEEDED` — safe *only because* of the stable key.
* **Circuit breaker per provider** (`breaker(method)`): a dead wallet provider
  doesn't fail card payments (tested). Open circuit → `NotSent` → decline,
  no unknowns piling up.
* `GetStatus` bypasses the breaker: the reconciler must be able to ask when writes fail.
* Eligibility channel is **connected eagerly** at boot — see bug below.

## State machine

```
AUTHORIZING ─► AUTHORIZED ─► CAPTURING ─► CAPTURED ─► REFUNDING ─► REFUNDED
     │              └──────► VOIDING ───► VOIDED
     └─► COMPENSATING ─► FAILED
```
`-ING` states are "in flight": the only ones the reconciler touches, the only
ones in the GSI. Capture/void/refund all share `settle()`: claim by moving to
the `-ING` state (that write is the lock), apply to each leg, finish.

## Real bugs found while building it (tell these)

1. **Lazy gRPC channel blew the eligibility deadline.** The first request after
   boot spent 246–291 ms *connecting* (logged: `connecting_and_lb_delay=291062800ns,
   was_still_waiting`) inside a 300 ms deadline → 503. Fix: `channel.getState(true)`
   at bean creation. Reproduced 2/2 with lazy connect, 0 with eager.
2. **My negative control was wrong first.** In unsafe mode I generated a fresh
   provider key per retry but still stored the *original* key on the leg, so
   capture asked the provider about a key it had never seen and the run hung in
   `CAPTURING`. A naive real implementation stores the reference from whichever
   attempt answered — fixed the control to do that, and *then* the orphaned
   earlier holds show up as double charges. Lesson: a control that fails for
   the wrong reason proves nothing.

## Honest scope (say it before they ask)

* Providers are **mocks** with injected faults; no card network, no 3DS, no
  partial captures/refunds, single currency per payment.
* "DynamoDB" = **DynamoDB Local** (in-process in tests/harness, container in
  compose). Not deployed to AWS. Table creation is in app code for local/CI;
  in AWS it would be Terraform/CDK. DynamoDB Local doesn't actually expire TTL items.
* Crash recovery **rolls back** a payment interrupted mid-authorization unless
  every leg had already landed. Rolling forward (authorizing the remaining legs)
  is possible but the customer's request is gone, so I chose the simpler, safer rule.
* Eligibility is a rules-as-config evaluator, not a real risk engine.
* No auth on the API; `/internal/*` would be on an internal listener.
* Everything runs on one machine in the benchmarks, load generator included.

## Likely follow-ups

* *Exactly-once?* No — at-least-once delivery + idempotent effects = effectively once.
* *Why not a DynamoDB transaction for every save?* One item per payment makes it unnecessary; the transaction is only used where two items must appear together (key + payment).
* *Hot key?* A single payment is updated a handful of times; the only shared key was the GSI partition, hence sharding.
* *Why gRPC internally, REST externally?* Typed contract + codegen + deadlines/propagation between services; REST/JSON for browser/mobile clients.
* *Multiple replicas?* Every replica runs the reconciler; optimistic locking makes double pickup harmless (one gets `StaleVersionException`). At scale you'd shard the sweep or use leases.
* *Outbox / events?* Not built. Would add DynamoDB Streams → event bus for "payment captured" so booking/fulfilment react, instead of polling.
