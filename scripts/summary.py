#!/usr/bin/env python3
"""Render results/*.json as the markdown table that goes in the CI job summary."""
import json
import pathlib
import sys

sys.stdout.reconfigure(encoding="utf-8")

root = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "results")
out = []


def chaos(name):
    f = root / f"{name}.json"
    if not f.exists():
        return
    d = json.loads(f.read_text())
    v, db, c = d["verdict"], d["database"], d["client"]
    out.append(f"### {name} — {d['mode']}\n")
    out.append("| metric | value |\n|---|---|")
    rows = [
        ("orders / payments in DB", f"{d['config']['orders']} / {db['payments']}"),
        ("payment-service kill -9s", d["chaos"]["paymentServiceKills"]),
        ("client requests (retries on errors + 202 polls)", f"{c['httpRequests']} ({c['connectionErrorsRetried']} conn errors, {c['inFlight202Polls']} 202s)"),
        ("concurrent duplicate submits", c["concurrentDuplicatesSent"]),
        ("provider retries deduplicated by key", d["provider"]["retriesDeduplicatedByIdempotencyKey"]),
        ("**double-charged orders**", f"**{v['doubleChargedOrders']}**"),
        ("**ledger discrepancies**", f"**{v['ledgerDiscrepancies']}**"),
        ("duplicate payments for one order", db["duplicatePaymentsForSameOrder"]),
        ("approved − accounted (minor units)", d["money"]["approvedMinusAccounted"]),
        ("split-tender compensations needed / done",
         f"{d['splitTender']['giftCardChargedThenPaymentFailed']} / {d['splitTender']['giftCardCompensated']}"),
        ("stuck in flight after settle", db["stuckInFlight"]),
        ("final statuses", ", ".join(f"{k} {n}" for k, n in db["byStatus"].items())),
        ("pass", v["pass"]),
    ]
    out.extend(f"| {k} | {val} |" for k, val in rows)
    out.append("")


chaos("chaos")
chaos("chaos-unsafe")

b = root / "bench.json"
if b.exists():
    d = json.loads(b.read_text())
    out.append(f"### latency (open loop, {d['machine']['cpus']} CPUs, everything on one box)\n")
    out.append("| call | target rps | achieved | p50 ms | p99 ms | p99.9 ms | max ms | failed |\n|---|---|---|---|---|---|---|---|")
    for key, label in (("eligibility_grpc", "eligibility gRPC"), ("authorize_http", "POST /v1/payments")):
        for r in d[key]:
            out.append(f"| {label} | {r['targetRps']} | {r['achievedRps']} | {r['p50ms']} | {r['p99ms']} | {r['p999ms']} | {r['maxMs']} | {r['failed']} |")
    out.append("")

print("\n".join(out))
