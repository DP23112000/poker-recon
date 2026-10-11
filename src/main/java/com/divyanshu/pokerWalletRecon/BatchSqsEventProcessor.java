package com.divyanshu.pokerWalletRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

public class BatchSqsEventProcessor implements RequestHandler<SQSEvent, Void> {

    private static final Logger logger = LoggerFactory.getLogger(BatchSqsEventProcessor.class);

    private static CqlSession cqlSession;
    private static PreparedStatement insertStatement;

    static {
        try {
            cqlSession = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(System.getenv("CASSANDRA_HOST"), 9042))
                    .withLocalDatacenter("ap-south")
                    .withKeyspace(System.getenv("DB_KEYSPACE"))
                    .build();

            insertStatement = cqlSession.prepare(
                    "INSERT INTO " + System.getenv("DB_TABLE") + " (referenceid, body) VALUES (?, ?)"
            );
        } catch (Exception exception) {
            logger.error("CRITICAL: Failed to initialize Cassandra session or prepare statement", exception);
        }
    }

    @Override
    public Void handleRequest(SQSEvent sqsEvent, Context context) {
        if (sqsEvent == null || sqsEvent.getRecords() == null || sqsEvent.getRecords().isEmpty()) {
            logger.info("No SQS events provided in the current batch.");
            return null;
        }

        Set<String> processedMessageIds = new HashSet<>();
        BatchStatement batchStatement = BatchStatement.builder(BatchType.LOGGED).build();

        for (SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
            String messageId = message.getMessageId();

            if (!processedMessageIds.add(messageId)) {
                logger.debug("Skipping duplicate message ID within current execution: {}", messageId);
                continue;
            }

            batchStatement = batchStatement.add(insertStatement.bind(messageId, message.getBody()));
        }

        try {
            if (batchStatement.size() > 0) {
                cqlSession.execute(batchStatement);
                logger.info("Successfully batched and inserted {} records into Cassandra.", batchStatement.size());
            }
        } catch (Exception executionException) {
            logger.error("Failed to execute Cassandra batch insert for SQS events.", executionException);
            throw new RuntimeException("Cassandra batch insert failed. SQS messages will remain in queue.", executionException);
        }

        return null;
    }
}