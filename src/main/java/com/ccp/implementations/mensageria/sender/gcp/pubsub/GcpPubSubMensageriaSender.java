package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.especifications.main.authentication.CcpAuthenticationProvider;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

class GcpPubSubMensageriaSender implements CcpMensageriaSender {
//	private static String PROJECT_ID = ServiceOptions.getDefaultProjectId();;
	private static String PROJECT_ID = "jn-hmg";

	private static final Map<String, Publisher> publishers = new HashMap<>();

	private Publisher getPublisher(String topicId) {
		
		boolean alreadyCalledBefore = publishers.containsKey(topicId);
		
		if(alreadyCalledBefore) {
			Publisher publisher = publishers.get(topicId);
			return publisher;
		}
		
		ProjectTopicName topico = ProjectTopicName.newBuilder().setProject(PROJECT_ID).setTopic(topicId).build();
		Publisher publisher = null;
		
		try {
			publisher = Publisher.newBuilder(topico).build();
			publishers.put(topicId, publisher);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return publisher;
	}

	public void send(Enum<?> topicName, String... msgs) {
		Publisher publisher = getPublisher(topicName.name()); 

		try {
			for (String json : msgs) {
				ByteString data = ByteString.copyFrom(json.getBytes(StandardCharsets.UTF_8));
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
				publisher.publish(pubsubMessage);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void send1(Enum<?> topicName , String... msgs) {
		List<String> asList = Arrays.asList(msgs);
		List<CcpMapDecorator> messages = asList.stream().map(message -> this.map(message)).collect(Collectors.toList());
		String url = "https://pubsub.googleapis.com/v1/projects/"
				+ PROJECT_ID
				+ "/topics/"
				+ topicName
				+ ":publish";
		
		CcpAuthenticationProvider authenticationProvider = CcpDependencyInjection.getDependency(CcpAuthenticationProvider.class);
		String token = authenticationProvider.getJwtToken();
		
		CcpMapDecorator body = new CcpMapDecorator().put("messages", messages);
		
		CcpHttpHandler ccpHttpHandler = new CcpHttpHandler(200);
		CcpMapDecorator authorization = new CcpMapDecorator().put("Authorization", "Bearer " + token);
		ccpHttpHandler.executeHttpRequest(url, "POST", authorization, body, CcpHttpResponseType.singleRecord);
	}

	private CcpMapDecorator map(String message) {
		Encoder encoder = Base64.getEncoder();
		byte[] bytes = message.getBytes();
		byte[] encode = encoder.encode(bytes);
		String value = new String( encode);
		CcpMapDecorator json = new CcpMapDecorator().put("data", value);
		return json;
	}
}
