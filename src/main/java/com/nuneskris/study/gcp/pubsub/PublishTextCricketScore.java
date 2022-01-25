package com.nuneskris.study.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PublishTextCricketScore {
    public static void main(String... args) throws Exception {
        PublishTextCricketScore publishCricketScore = new  PublishTextCricketScore();
        try {
            publishCricketScore.publisherExample("java-maven-dataflow","my-topic");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            e.printStackTrace();
        }
    }
    public void publisherExample(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException, CsvValidationException {
        Publisher publisher = null;

        try{

        TopicName topicName = TopicName.of(projectId, topicId);
        // Create a publisher and set message ordering to true.

        publisher =  Publisher.newBuilder(topicName)
                // Sending messages to the same region ensures they are received in order
                // even when multiple publishers are used.
                //   .setEndpoint("us-east1-pubsub.googleapis.com:443")
                .setEnableMessageOrdering(true)
                .build();
        try (CSVReader reader = new CSVReader(new FileReader("src/main/resources/IPLBall-by-Ball 2008-2020.csv"))) {
            String[] lineInArray;
            int limitTesting = 0;
            while ((lineInArray = reader.readNext()) != null && limitTesting++ <10) {

                String message = lineInArray[0] + "," +
                        lineInArray[1]  + "," +
                        lineInArray[2]  + "," +
                        lineInArray[3]   + "," +
                        lineInArray[4]  + "," +
                        lineInArray[5] + "," +
                        lineInArray[6]  + "," +
                        lineInArray[7];
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                ApiFuture<String> future = publisher.publish(pubsubMessage);
                // Add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(
                        future,
                        new ApiFutureCallback<String>() {

                            @Override
                            public void onFailure(Throwable throwable) {
                                if (throwable instanceof ApiException) {
                                    ApiException apiException = ((ApiException) throwable);
                                    // details on the API exception
                                    System.out.println(apiException.getStatusCode().getCode());
                                    System.out.println(apiException.isRetryable());
                                }
                                System.out.println("Error publishing message : ");
                            }

                            @Override
                            public void onSuccess(String messageId) {
                                // Once published, returns server-assigned message ids (unique within the topic)
                                System.out.println("Published message ID: " + messageId);
                            }
                        },
                        MoreExecutors.directExecutor());

            }
        }

        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }

    }
}
