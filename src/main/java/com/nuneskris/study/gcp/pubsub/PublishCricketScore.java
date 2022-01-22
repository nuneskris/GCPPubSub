package com.nuneskris.study.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
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

public class PublishCricketScore {

    private  final AvroCoder<CricketDelivery> CODER = AvroCoder.of(CricketDelivery.class);

    public static void main(String... args) throws Exception {
        PublishCricketScore publishCricketScore = new  PublishCricketScore();
        try {
            publishCricketScore.publisherExample("java-maven-dataflow","avro-topic");
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

    public  void publisherExample(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException, CsvValidationException {
        Publisher publisher = null;
        try {

            TopicName topicName = TopicName.of(projectId, topicId);
            // Create a publisher and set message ordering to true.

            publisher =  Publisher.newBuilder(topicName)
                            // Sending messages to the same region ensures they are received in order
                            // even when multiple publishers are used.
                         //   .setEndpoint("us-east1-pubsub.googleapis.com:443")
                            .setEnableMessageOrdering(true)
                            .build();

            try (CSVReader reader = new CSVReader(new FileReader("file.csv"))) {
                String[] lineInArray;
                while ((lineInArray = reader.readNext()) != null) {
                    System.out.println(lineInArray[0] + lineInArray[1] + "etc...");
                }
            }

            CricketScore score = CricketScore.newBuilder()
                    .setId("12345")
                    .setBall(1)
                    .setBatsman("Kris")
                    .setBatsmanRuns(1)
                    .setBattingTeam("Nunes")
                    .setBowler("Juan")
                    .setBowlingTeam("Roach")
                    .setDismissalKind(null)
                    .build();

            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().jsonEncoder(CricketScore.getClassSchema(), byteStream);
            // Encode the object and write it to the output stream.
            score.customEncode(encoder);
            encoder.flush();
            // Publish the encoded object as a Pub/Sub message.
            ByteString data = ByteString.copyFrom(byteStream.toByteArray());
            PubsubMessage message = PubsubMessage.newBuilder().setData(data).build();
            System.out.println("Publishing message: " + message);

            ApiFuture<String> future = publisher.publish(message);
            System.out.println("Published message ID: " + future.get());


        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }


}
