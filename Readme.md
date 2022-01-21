gcloud pubsub topics create my-topic

gcloud pubsub subscriptions create my-sub --topic my-topic

cd $HOME && git clone https://github.com/nuneskris/GCPPubSub.git

cd GCPPubSub

mvn compile exec:java -Dexec.mainClass=com.nuneskris.study.gcp.pubsub.PublisherExample -Dexec.args="java-maven-dataflow my-topic"
