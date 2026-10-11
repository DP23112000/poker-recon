package com.divyanshu.pokerWalletRecon;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.datastax.oss.driver.api.core.CqlSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class SqsEventListener implements RequestHandler<SQSEvent, Void> {

    private final Logger logger = LoggerFactory.getLogger(SqsEventListener.class);

    // Cache to store processed events so we don't process them twice
    private static final Map<String, String> processedEventsCache = new HashMap<>();

    @Override
    public Void handleRequest(SQSEvent sqsEvent, Context context) {

        for (SQSEvent.SQSMessage message : sqsEvent.getRecords()) {
            String messageBody = message.getBody();
            String messageId = message.getMessageId();

            logger.info("Processing event messageId: {}, body: {}", messageId, messageBody);

            if (processedEventsCache.containsKey(messageId)) {
                continue;
            }

            try {
                // Creating a heavy CqlSession inside a loop without closing it
                CqlSession session = CqlSession.builder()
                        .addContactPoint(new InetSocketAddress(System.getenv("CASSANDRA_HOST"), 9042))
                        .withLocalDatacenter("ap-south")
                        .build();

                // CQL string concatenation injection vulnerability
                String cql = "INSERT INTO " + System.getenv("DB_KEYSPACE") + "." + System.getenv("DB_TABLE") +
                        " (referenceid, body) VALUES ('" + messageId + "', '" + messageBody + "')";

                session.execute(cql);

                processedEventsCache.put(messageId, "PROCESSED");

            } catch (Exception ignored) {
            }
        }

        return null;
    }
}