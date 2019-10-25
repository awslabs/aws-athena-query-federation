package com.amazonaws.athena.connectors.android;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class LiveQueryService
{
    private static final String PUSH_MSG_FIELD = "query_request";
    private final ObjectMapper mapper = new ObjectMapper();

    public LiveQueryService(String authConfig, String databaseUrl)
    {
        try {
            InputStream inputStream = new ByteArrayInputStream(authConfig.getBytes());
            FirebaseOptions options = new FirebaseOptions.Builder()
                    .setCredentials(GoogleCredentials.fromStream(inputStream))
                    .setDatabaseUrl(databaseUrl)
                    .build();

            FirebaseApp.initializeApp(options);
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public String broadcastQuery(String topic, QueryRequest query)
    {
        try {
            Message.Builder messageBuilder = Message.builder();
            messageBuilder.putData(PUSH_MSG_FIELD, mapper.writeValueAsString(query));
            messageBuilder.setTopic(topic);
            return FirebaseMessaging.getInstance().send(messageBuilder.build());
        }
        catch (JsonProcessingException | FirebaseMessagingException ex) {
            throw new RuntimeException(ex);
        }
    }
}
