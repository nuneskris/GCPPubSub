gcloud pubsub topics create my-topic

gcloud pubsub subscriptions create my-sub --topic my-topic

cd $HOME && git clone https://github.com/nuneskris/GCPPubSub.git

cd GCPPubSub

mvn compile exec:java -Dexec.mainClass=com.nuneskris.study.gcp.pubsub.PublisherExample -Dexec.args="java-maven-dataflow my-topic"

-----------------------------------------------

git pull origin master



-------------------
#Avro Publishing

The avsc schema file is used to generate a utility file for avro encoding. Refer below link
https://avro.apache.org/docs/current/gettingstartedjava.html

GCP Dataflow PubSubIO only reads Binary format
    public static PubsubIO.Read<GenericRecord> readAvroGenericRecords(Schema avroSchema)
        Returns a PTransform that continuously reads binary encoded Avro messages into the Avro GenericRecord type.
        Beam will infer a schema for the Avro schema. This allows the output to be used by SQL and by the schema-transform library.

PublishAvroRecordsExample --> GCP Docs example

The below example publishes cricket score.
mvn compile exec:java -Dexec.mainClass=com.nuneskris.study.gcp.pubsub.PublishCricketScore