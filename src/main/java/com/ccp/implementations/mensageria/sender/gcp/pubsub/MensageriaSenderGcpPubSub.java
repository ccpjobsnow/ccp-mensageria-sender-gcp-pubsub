package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.ccp.decorators.CcpStringDecorator;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

class MensageriaSenderGcpPubSub implements CcpMensageriaSender {

	private static final Map<Enum<?>, Publisher> publishers = new HashMap<>();
	private static String PROJECT_ID = ServiceOptions.getDefaultProjectId();
	private final FixedCredentialsProvider credentialsProvider;
	public MensageriaSenderGcpPubSub() {
		try {
			InputStream credentialsFile = new CcpStringDecorator("credentials.json").inputStreamFrom().classLoader();
			GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsFile);
			this.credentialsProvider = FixedCredentialsProvider.create(credentials);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private Publisher getPublisher(Enum<?> topicId) {

		if(publishers.containsKey(topicId)) {
			Publisher publisher = publishers.get(topicId);
			return publisher;
		}
	
		
		ProjectTopicName topico = ProjectTopicName.newBuilder().setProject(PROJECT_ID).setTopic(topicId.name()).build();
		
		try {
			Publisher publisher = null;
			publisher = Publisher.newBuilder(topico).setCredentialsProvider(this.credentialsProvider).build();
			publishers.put(topicId, publisher);
			return publisher;
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
		
	
	@Override
	public void send(String json, Enum<?> topic) {
		Publisher publisher = this.getPublisher(topic); 

		try {
			ByteString data = ByteString.copyFrom(json.getBytes(StandardCharsets.UTF_8));
			PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
			publisher.publish(pubsubMessage);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
