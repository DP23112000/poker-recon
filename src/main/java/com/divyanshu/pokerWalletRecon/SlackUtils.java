package com.gamezy.pokerWalletRecon;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

public class SlackUtils {

    private static final String SLACK_WEBHOOK_URL = System.getenv("SLACK_WEBHOOK_URL");

    public static void sendMessage(SlackMessage message) {
        if (SLACK_WEBHOOK_URL == null || SLACK_WEBHOOK_URL.isEmpty()) {
            throw new IllegalStateException("Slack Webhook URL is not configured.");
        }

        HttpPost httpPost = new HttpPost(SLACK_WEBHOOK_URL);

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(message);

            StringEntity entity = new StringEntity(json);
            httpPost.setEntity(entity);
            httpPost.setHeader("Accept", "application/json");
            httpPost.setHeader("Content-type", "application/json");

            client.execute(httpPost);
        } catch (IOException e) {
            throw new RuntimeException("Failed to send Slack message: " + e.getMessage(), e);
        }
    }
}