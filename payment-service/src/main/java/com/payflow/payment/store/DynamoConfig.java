package com.payflow.payment.store;

import com.payflow.payment.PayflowProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;

import java.net.URI;

@Configuration
public class DynamoConfig {

    private static final Logger log = LoggerFactory.getLogger(DynamoConfig.class);

    @Bean(destroyMethod = "close")
    DynamoDbClient dynamoDbClient(PayflowProperties props) {
        PayflowProperties.Dynamo d = props.dynamo();
        DynamoDbClientBuilder b = DynamoDbClient.builder()
                .region(Region.of(d.region()))
                .httpClientBuilder(UrlConnectionHttpClient.builder());
        if (d.endpoint() != null && !d.endpoint().isBlank()) {
            // DynamoDB Local accepts any credentials
            b.endpointOverride(URI.create(d.endpoint()))
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")));
        }
        DynamoDbClient db = b.build();
        // Local/CI convenience, done before any bean can use the table.
        // In a real account the table belongs to Terraform/CDK.
        if (d.createTable()) createTableIfMissing(db, d.table());
        return db;
    }

    public static void createTableIfMissing(DynamoDbClient db, String table) {
        try {
            db.createTable(CreateTableRequest.builder()
                    .tableName(table)
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(
                            attr("pk", ScalarAttributeType.S), attr("sk", ScalarAttributeType.S),
                            attr("gsi1pk", ScalarAttributeType.S), attr("gsi1sk", ScalarAttributeType.N))
                    .keySchema(key("pk", KeyType.HASH), key("sk", KeyType.RANGE))
                    .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                            .indexName(PaymentStore.GSI)
                            .keySchema(key("gsi1pk", KeyType.HASH), key("gsi1sk", KeyType.RANGE))
                            .projection(Projection.builder().projectionType(ProjectionType.KEYS_ONLY).build())
                            .build())
                    .build());
            db.updateTimeToLive(UpdateTimeToLiveRequest.builder().tableName(table)
                    .timeToLiveSpecification(TimeToLiveSpecification.builder()
                            .attributeName("expiresAt").enabled(true).build()).build());
            log.info("created table {}", table);
        } catch (ResourceInUseException e) {
            log.info("table {} already exists", table);
        }
    }

    private static AttributeDefinition attr(String name, ScalarAttributeType type) {
        return AttributeDefinition.builder().attributeName(name).attributeType(type).build();
    }

    private static KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }
}
