# payflow

A payment orchestration backend in **Java 21 / Spring Boot 3 / gRPC / DynamoDB**,
shaped like a travel checkout: decide which payment methods a hotel / flight /
car checkout may offer, then take the money (card, wallet, gift card, or a
gift card + card split), without ever charging twice, even when providers
time out and the service is `kill -9`ed mid-payment.

```
             REST + Idempotency-Key                 gRPC (deadline 300 ms)
  client ───────────────────────────► payment-service ───────────────────► eligibility-service
                                       │  saga + reconciler                  rules-as-config
                                       │
                     gRPC, per-provider│breaker, retry w/ same key
                                       ▼
                                   mock-psp  (card / wallet / gift card,
                                              fault injection, provider-side ledger)
                                       │
                         DynamoDB ◄────┘ single table, optimistic locking, sparse GSI
```

| module | what |
|---|---|
| `proto/` | `payflow.proto`: Eligibility, PaymentProvider, PspAdmin services |
| `eligibility-service/` | Spring Boot + grpc-java server (health + reflection). Rules per method in `application.yml`: currency, country allow/block, device, line of business, amount cap |
| `payment-service/` | REST checkout API, payment saga, reconciler, DynamoDB store, gRPC clients with Resilience4j |
| `mock-psp/` | Fake provider. Authorize is idempotent on key; Void on an unknown key writes a tombstone. Injects declines, `UNAVAILABLE`, lost requests and **timeout-after-commit** |
| `loadtest/` | Chaos run (provider faults + `kill -9` + ledger audit) and open-loop latency bench. Starts everything itself |

## What it does about the hard parts

* **Idempotency in two places.** The `Idempotency-Key` item and the payment are created in one DynamoDB
  `TransactWriteItems`, both conditional on not existing, so concurrent duplicates collapse to one payment
  (same key + different body → 422). Each tender's provider key is *derived* from the payment id
  (`paymentId:leg`), so retries, restarts and the reconciler all address the same authorization.
* **Unknown outcomes.** A provider timeout is not a failure. The leg becomes `UNKNOWN`, the API answers
  **202**, and the reconciler later asks the provider (`GetStatus`) and rolls forward or back.
  Compensation voids keys the provider has never seen, leaving a tombstone, so a late request can't place a hold.
* **Split tender as a saga.** Gift card first; if the card is declined the gift card is voided.
  `COMPENSATING` is persisted before any void, so a crash mid-rollback is finished by the reconciler.
* **One item per payment + optimistic locking** (`version`): every state change is one conditional write.
  Capture-vs-void races have exactly one winner (the other gets 409).
* **Sparse, sharded GSI** holds only in-flight payments: it *is* the reconciler's work queue.
* **Resilience4j**: deadline per attempt, retry with jittered backoff only on `UNAVAILABLE`/`DEADLINE_EXCEEDED`,
  a circuit breaker **per provider** (an open circuit is "not sent", so a clean decline, not an unknown).

Design notes and the interview-style walkthrough: [docs/NOTES.md](docs/NOTES.md).

## Results

Measured locally (8-thread Windows laptop with ~1 GB free RAM; every service, DynamoDB Local and the load
generator on the same box). Raw JSON in [results/](results/); CI re-runs these on Linux and posts them
in the job summary.

**Chaos run** (`results/chaos.json`): 2,000 checkouts at 32 concurrent clients. The provider declines 8%,
drops 3% of requests, returns `UNAVAILABLE` on 3%, and on 5% **commits then hangs past the 500 ms deadline**.
20% of checkouts are double-submitted concurrently, clients retry on every error and 202, and
payment-service was `kill -9`ed and restarted **5 times** mid-run (3,260 client requests hit a dead server).

| check (our DB vs the provider's own ledger) | safe | unsafe control¹ |
|---|---|---|
| orders → payments created | 2,000 → 2,000 | 500 → 630 |
| **orders charged twice** | **0** | 117 |
| **ledger discrepancies** (leg mismatch, orphan hold, inconsistent or stuck payment) | **0** | 53 |
| money approved by provider − money we account for | **0** | 1,916,391 minor units |
| split tender: gift card charged then payment failed → gift card voided | **68 / 68** | 24 / 24 |
| retries that reached the provider and were deduplicated by key | 127 | 0 |
| payments still in flight after settling | 0 | 0 |

¹ Same harness with `payflow.unsafe-mode=true`: no API idempotency, new provider key per retry. It exists to
show the audit *can* fail. A check that has never caught a double charge proves nothing.

**Latency** (`results/bench.json`, open loop: requests fire on schedule and latency is measured from the
intended send time, so a slow server can't hide its own tail):

| call | rate | p50 | p99 | p99.9 | errors |
|---|---|---|---|---|---|
| eligibility gRPC | 500 / 1,000 / 2,000 rps | 9.3 / 10.0 / 11.1 ms | 21.0 / 22.5 / 23.8 ms | 27.5 / 29.1 / 29.1 ms | 0 |
| `POST /v1/payments` (card authorize, end to end) | 50 / 100 / 200 rps | 79.8 / 78.4 / 80.9 ms | 113 / 113 / 141 ms | 122 / 126 / 157 ms | 0 |

Authorize makes 4 DynamoDB Local round trips plus 2 gRPC calls. DynamoDB Local is a SQLite-backed
emulator, so these numbers say "no tail blow-up up to 200 rps on a laptop", not anything about real DynamoDB.

Tests: 22 (13 payment-service integration tests over real HTTP + gRPC + DynamoDB Local, 5 eligibility gRPC, 4 mock-provider).

## Run it

Requires JDK 21 and Maven. No Docker needed: tests and the harness run DynamoDB Local in-process.

```bash
mvn verify                       # 22 tests: real gRPC, real DynamoDB Local, real mock provider

mvn package -DskipTests
H="java -Dsqlite4java.library.path=loadtest/target/native-libs -jar loadtest/target/loadtest.jar"
$H chaos --orders=2000 --kills=8          # must report pass: true
$H chaos --unsafe --orders=500 --kills=3  # negative control: must find double charges
$H bench
python3 scripts/summary.py results

docker compose up --build        # or the containers: http://localhost:8080
curl -s localhost:8080/v1/payments -H 'Content-Type: application/json' -H 'Idempotency-Key: k1' -d '{
  "orderRef":"o1","country":"US","currency":"USD","lob":"HOTEL","device":"WEB","amountMinor":25000,
  "tenders":[{"method":"GIFT_CARD","amountMinor":5000,"token":"tok_gc"},
             {"method":"CARD","amountMinor":20000,"token":"tok_visa"}]}'
```

Card token `tok_decline...` forces a decline (try it on the split payment to watch the gift card get voided).

## API

| call | result |
|---|---|
| `POST /v1/payments` (header `Idempotency-Key`) | 201 authorized · 202 outcome unknown, retry same key · 402 declined, nothing held · 422 ineligible / key reused with another body · 503 eligibility down, safe to retry |
| `GET /v1/payments/{id}` | current state |
| `POST /v1/payments/{id}/capture` · `/void` · `/refund` | 200 done · 202 in flight · 409 illegal from this state or lost a race |

## Honest scope

* Providers are **mocks** with injected faults. No real card network, 3DS, partial capture or multi-currency.
* "DynamoDB" means **DynamoDB Local** (in-process for tests/harness, the official container in compose).
  Not deployed to AWS. Table creation lives in app code for local/CI only.
* A payment interrupted mid-authorization is rolled **back** unless every leg had already landed.
* Benchmarks run every service, DynamoDB Local and the load generator on one machine.
