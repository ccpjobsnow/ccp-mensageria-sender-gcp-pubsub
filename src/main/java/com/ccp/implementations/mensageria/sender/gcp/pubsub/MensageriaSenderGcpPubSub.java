package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

class MensageriaSenderGcpPubSub implements CcpMensageriaSender {

	private static final Map<String, Publisher> publishers = new HashMap<>();
	private static String PROJECT_ID = System.getenv("tentant");


	private static Publisher getPublisher(String topicId) {

		if(publishers.containsKey(topicId)) {
			Publisher publisher = publishers.get(topicId);
			return publisher;
		}
	
		
		ProjectTopicName topico = ProjectTopicName.newBuilder().setProject(PROJECT_ID).setTopic(topicId).build();
		
		try {
			Publisher publisher = null;
			publisher = Publisher.newBuilder(topico).build();
			publishers.put(topicId, publisher);
			return publisher;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}


	}
		
	
	@Override
	public void send(String json, String topic) {
		Publisher publisher = getPublisher(topic); 

		try {
			ByteString data = ByteString.copyFrom(json.getBytes(StandardCharsets.UTF_8));
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
			System.out.println("publicou");
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
}
