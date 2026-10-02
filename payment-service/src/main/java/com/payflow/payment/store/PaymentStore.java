package com.payflow.payment.store;

import com.payflow.payment.PayflowProperties;
import com.payflow.payment.domain.Payment;
import com.payflow.payment.domain.PaymentStatus;
import com.payflow.payment.domain.Tender;
import com.payflow.proto.Device;
import com.payflow.proto.LineOfBusiness;
import com.payflow.proto.PaymentMethod;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Single-table DynamoDB model. Access patterns and key layout:
 *
 * <pre>
 * pk                 sk        what
 * PAY#{paymentId}    PAYMENT   the payment + its tenders (one item, versioned)
 * IDEM#{key}         IDEM      Idempotency-Key -> paymentId + request hash, TTL 24h
 *
 * GSI "inflight" (sparse, KEYS_ONLY):
 * gsi1pk = INFLIGHT#{0..3}   gsi1sk = updatedAt millis
 * </pre>
 *
 * Only in-flight payments carry gsi1pk, so the index holds exactly the work
 * the reconciler may need to pick up and stays tiny. It is sharded 4 ways so a
 * single partition key doesn't become a hot partition at high write rates.
 */
@Component
public class PaymentStore {

    public static final int SHARDS = 4;
    public static final String GSI = "inflight";
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    /** Another writer changed the item since we read it. */
    public static class StaleVersionException extends RuntimeException {
        public StaleVersionException(String id) {
            super("payment " + id + " was modified concurrently");
        }
    }

    public record IdempotencyRecord(String paymentId, String requestHash) {}

    private final DynamoDbClient db;
    private final String table;

    public PaymentStore(DynamoDbClient db, PayflowProperties props) {
        this.db = db;
        this.table = props.dynamo().table();
    }

    // ------------------------------------------------------------------ writes

    /**
     * Writes the idempotency record and the new payment in ONE transaction,
     * both conditional on not existing. Either both land or neither does, so
     * there is never a key pointing at no payment or a payment with no key.
     *
     * @return empty if we created it; otherwise the record that already owns the key
     */
    public Optional<IdempotencyRecord> createWithIdempotencyKey(String key, String requestHash, Payment p) {
        Map<String, AttributeValue> idem = new HashMap<>();
        idem.put("pk", s("IDEM#" + key));
        idem.put("sk", s("IDEM"));
        idem.put("paymentId", s(p.id()));
        idem.put("requestHash", s(requestHash));
        idem.put("expiresAt", n(Instant.now().plus(IDEMPOTENCY_TTL).getEpochSecond()));
        try {
            db.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(
                    TransactWriteItem.builder().put(Put.builder().tableName(table).item(idem)
                            .conditionExpression("attribute_not_exists(pk)").build()).build(),
                    TransactWriteItem.builder().put(Put.builder().tableName(table).item(toItem(p))
                            .conditionExpression("attribute_not_exists(pk)").build()).build()
            ).build());
            return Optional.empty();
        } catch (TransactionCanceledException e) {
            List<CancellationReason> reasons = e.cancellationReasons();
            if (!reasons.isEmpty() && "ConditionalCheckFailed".equals(reasons.get(0).code())) {
                // Lost the race to a concurrent request with the same key: return the winner.
                return findIdempotency(key);
            }
            throw e;
        }
    }

    /** Plain insert, used only by unsafe mode (no idempotency record). */
    public void create(Payment p) {
        db.putItem(PutItemRequest.builder().tableName(table).item(toItem(p))
                .conditionExpression("attribute_not_exists(pk)").build());
    }

    /**
     * Optimistic-locked save: succeeds only if the stored version is still the
     * one {@code p} was read at. Returns the saved copy (version + 1).
     */
    public Payment save(Payment p) {
        Payment next = p.nextVersion(Instant.now());
        try {
            db.putItem(PutItemRequest.builder().tableName(table).item(toItem(next))
                    .conditionExpression("version = :v")
                    .expressionAttributeValues(Map.of(":v", n(p.version())))
                    .build());
            return next;
        } catch (ConditionalCheckFailedException e) {
            throw new StaleVersionException(p.id());
        }
    }

    // ------------------------------------------------------------------- reads

    public Optional<IdempotencyRecord> findIdempotency(String key) {
        Map<String, AttributeValue> item = db.getItem(GetItemRequest.builder().tableName(table)
                .key(Map.of("pk", s("IDEM#" + key), "sk", s("IDEM"))).consistentRead(true).build()).item();
        if (item == null || item.isEmpty()) return Optional.empty();
        return Optional.of(new IdempotencyRecord(item.get("paymentId").s(), item.get("requestHash").s()));
    }

    public Optional<Payment> get(String id) {
        Map<String, AttributeValue> item = db.getItem(GetItemRequest.builder().tableName(table)
                .key(Map.of("pk", s("PAY#" + id), "sk", s("PAYMENT"))).consistentRead(true).build()).item();
        return item == null || item.isEmpty() ? Optional.empty() : Optional.of(fromItem(item));
    }

    /**
     * Ids of in-flight payments not touched since {@code cutoff}. The GSI is
     * eventually consistent, so callers must re-read each one with {@link #get}
     * before acting on it.
     */
    public List<String> findStaleInFlight(Instant cutoff, int limitPerShard) {
        List<String> ids = new ArrayList<>();
        for (int shard = 0; shard < SHARDS; shard++) {
            var items = db.query(QueryRequest.builder().tableName(table).indexName(GSI)
                    .keyConditionExpression("gsi1pk = :p AND gsi1sk < :c")
                    .expressionAttributeValues(Map.of(":p", s("INFLIGHT#" + shard), ":c", n(cutoff.toEpochMilli())))
                    .limit(limitPerShard).build()).items();
            for (var it : items) ids.add(it.get("pk").s().substring("PAY#".length()));
        }
        return ids;
    }

    /** Full table scan of payments, for the chaos harness's final audit only. */
    public List<Payment> scanAll() {
        List<Payment> out = new ArrayList<>();
        Map<String, AttributeValue> start = null;
        do {
            var res = db.scan(ScanRequest.builder().tableName(table)
                    .filterExpression("sk = :s").expressionAttributeValues(Map.of(":s", s("PAYMENT")))
                    .exclusiveStartKey(start).build());
            res.items().forEach(i -> out.add(fromItem(i)));
            start = res.hasLastEvaluatedKey() && !res.lastEvaluatedKey().isEmpty() ? res.lastEvaluatedKey() : null;
        } while (start != null);
        return out;
    }

    // ----------------------------------------------------------------- mapping

    static int shardOf(String id) {
        return Math.floorMod(id.hashCode(), SHARDS);
    }

    static Map<String, AttributeValue> toItem(Payment p) {
        Map<String, AttributeValue> m = new HashMap<>();
        m.put("pk", s("PAY#" + p.id()));
        m.put("sk", s("PAYMENT"));
        m.put("orderRef", s(p.orderRef()));
        m.put("country", s(p.country()));
        m.put("currency", s(p.currency()));
        m.put("lob", s(p.lob().name()));
        m.put("device", s(p.device().name()));
        m.put("amountMinor", n(p.amountMinor()));
        m.put("status", s(p.status().name()));
        m.put("version", n(p.version()));
        m.put("createdAt", n(p.createdAt().toEpochMilli()));
        m.put("updatedAt", n(p.updatedAt().toEpochMilli()));
        if (p.failureReason() != null) m.put("failureReason", s(p.failureReason()));
        if (p.status().inFlight) {
            m.put("gsi1pk", s("INFLIGHT#" + shardOf(p.id())));
            m.put("gsi1sk", n(p.updatedAt().toEpochMilli()));
        }
        List<AttributeValue> legs = new ArrayList<>();
        for (Tender t : p.tenders()) {
            Map<String, AttributeValue> leg = new HashMap<>();
            leg.put("method", s(t.method().name()));
            leg.put("amountMinor", n(t.amountMinor()));
            leg.put("token", s(t.token()));
            leg.put("status", s(t.status().name()));
            leg.put("pspKey", s(t.pspKey()));
            if (t.providerRef() != null) leg.put("providerRef", s(t.providerRef()));
            if (t.declineReason() != null) leg.put("declineReason", s(t.declineReason()));
            legs.add(AttributeValue.fromM(leg));
        }
        m.put("tenders", AttributeValue.fromL(legs));
        return m;
    }

    static Payment fromItem(Map<String, AttributeValue> m) {
        List<Tender> tenders = new ArrayList<>();
        for (AttributeValue v : m.get("tenders").l()) {
            Map<String, AttributeValue> t = v.m();
            tenders.add(new Tender(
                    PaymentMethod.valueOf(t.get("method").s()),
                    Long.parseLong(t.get("amountMinor").n()),
                    t.get("token").s(),
                    Tender.Status.valueOf(t.get("status").s()),
                    t.get("pspKey").s(),
                    str(t.get("providerRef")),
                    str(t.get("declineReason"))));
        }
        return new Payment(
                m.get("pk").s().substring("PAY#".length()),
                m.get("orderRef").s(),
                m.get("country").s(),
                m.get("currency").s(),
                LineOfBusiness.valueOf(m.get("lob").s()),
                Device.valueOf(m.get("device").s()),
                Long.parseLong(m.get("amountMinor").n()),
                PaymentStatus.valueOf(m.get("status").s()),
                tenders,
                str(m.get("failureReason")),
                Long.parseLong(m.get("version").n()),
                Instant.ofEpochMilli(Long.parseLong(m.get("createdAt").n())),
                Instant.ofEpochMilli(Long.parseLong(m.get("updatedAt").n())));
    }

    private static AttributeValue s(String v) {
        return AttributeValue.fromS(v);
    }

    private static AttributeValue n(long v) {
        return AttributeValue.fromN(Long.toString(v));
    }

    private static String str(AttributeValue v) {
        return v == null ? null : v.s();
    }
}
