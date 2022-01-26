package com.nuneskris.study.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.coders.AvroCoder;

import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import com.google.common.util.concurrent.MoreExecutors;
import utilities.State;

public class PublishCricketScore {

    private  final AvroCoder<CricketDelivery> CODER = AvroCoder.of(CricketDelivery.class);

    public static void main(String... args) throws Exception {
        PublishCricketScore publishCricketScore = new  PublishCricketScore();
        try {
            publishCricketScore.publisherExample("java-maven-dataflow","my-avro-topic");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (CsvValidationException e) {
            e.printStackTrace();
        }
    };

    public void publisherExample(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException, CsvValidationException {
        Publisher publisher = null;
        try {
            Encoding encoding = null;
            TopicName topicName = TopicName.of(projectId, topicId);
            // Create a publisher and set message ordering to true.

            // Get the topic encoding type.
            try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
                encoding = topicAdminClient.getTopic(topicName).getSchemaSettings().getEncoding();
            }

            publisher =  Publisher.newBuilder(topicName)
                    // Sending messages to the same region ensures they are received in order
                    // even when multiple publishers are used.
                    //   .setEndpoint("us-east1-pubsub.googleapis.com:443")
                    .setEnableMessageOrdering(true)
                    .build();
            Encoder encoder = null;
            // Prepare an appropriate encoder for publishing to the topic.
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            switch (encoding) {
                case BINARY:
                    System.out.println("Preparing a BINARY encoder...");
                    encoder = EncoderFactory.get().directBinaryEncoder(byteStream, /*reuse=*/ null);
                    break;

                case JSON:
                    System.out.println("Preparing a JSON encoder...");
                    encoder = EncoderFactory.get().jsonEncoder(State.getClassSchema(), byteStream);
                    break;
            }

            if(encoder != null){
                try (CSVReader reader = new CSVReader(new FileReader("src/main/resources/IPLBall-by-Ball 2008-2020.csv")))
                {
                    String[] lineInArray;
                    int limitTesting = 0;
                    while ((lineInArray = reader.readNext()) != null && limitTesting++ < 10) {

                        CricketScore score = CricketScore.newBuilder()
                                .setId(lineInArray[0])
                                .setInning(Integer.valueOf(lineInArray[1]))
                                .setOver(Integer.valueOf(lineInArray[2]))
                                .setBall(Integer.valueOf(lineInArray[3]))
                                .setBatsman(lineInArray[4])
                                .setNonStriker(lineInArray[5])
                                .setBowler(lineInArray[6])
                                .setBatsmanRuns(Integer.valueOf(lineInArray[7]))
                                .setExtraRuns(Integer.valueOf(lineInArray[8]))
                                .setTotalRuns(Integer.valueOf(lineInArray[9]))
                                .setNonBoundary(Integer.valueOf(lineInArray[10]))
                                .setIsWicket(Integer.valueOf(lineInArray[11]))
                                .setDismissalKind(lineInArray[12])
                                .setPlayerDismissed(lineInArray[13])
                                .setFielder(lineInArray[14])
                                .setExtrasType(lineInArray[15])
                                .setBattingTeam(lineInArray[16])
                                .setBowlingTeam(lineInArray[17])
                                .build();


                        // Encode the object and write it to the output stream.
                        score.customEncode(encoder);
                        encoder.flush();
                        // Publish the encoded object as a Pub/Sub message.
                        ByteString data = ByteString.copyFrom(byteStream.toByteArray());

                        PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();

                        ApiFuture<String> future = publisher.publish(message);
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
                                        System.out.println("Error publishing message : " + message);
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

            }


        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }


}
