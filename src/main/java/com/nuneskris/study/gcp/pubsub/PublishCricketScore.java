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

            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().jsonEncoder(CricketScore.getClassSchema(), byteStream);

            try (CSVReader reader = new CSVReader(new FileReader("src/main/resources/IPLBall-by-Ball 2008-2020.csv"))) {
                String[] lineInArray;
                int limitTesting = 0;
                while ((lineInArray = reader.readNext()) != null || limitTesting++ < 10) {
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
