package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpInputStreamDecorator;
import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.decorators.CcpStringDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.especifications.main.authentication.CcpAuthenticationProvider;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.ccp.especifications.mensageria.sender.CcpTopic;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

class GcpPubSubMensageriaSender implements CcpMensageriaSender {
//	private static String PROJECT_ID = ServiceOptions.getDefaultProjectId();;
	private static String PROJECT_ID = "jn-hmg";

	private static final Map<String, Publisher> publishers = new HashMap<>();

	private Publisher getPublisher(String topicName) {

		boolean alreadyCalledBefore = publishers.containsKey(topicName);

		if (alreadyCalledBefore) {
			Publisher publisher = publishers.get(topicName);
			return publisher;
		}
		Publisher publisher = null;
		try {
			CcpStringDecorator ccpStringDecorator = new CcpStringDecorator("GOOGLE_APPLICATION_CREDENTIALS");
			CcpInputStreamDecorator inputStreamFrom = ccpStringDecorator.inputStreamFrom();
			InputStream fromEnvironmentVariablesOrClassLoaderOrFile = inputStreamFrom.fromEnvironmentVariablesOrClassLoaderOrFile();
			GoogleCredentials credentials = GoogleCredentials.fromStream(fromEnvironmentVariablesOrClassLoaderOrFile);
			FixedCredentialsProvider create = FixedCredentialsProvider.create(credentials);
			publisher = Publisher.newBuilder(topicName).setCredentialsProvider(create).build();
			publishers.put(topicName, publisher);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return publisher;
	}

	public void send2(Enum<?> topicName, String... msgs) {
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

	public void send(CcpTopic topicId, String...msgs)
			{
		Publisher publisher = getPublisher(topicId.getTopicName());

		try {

			List<String> messages = Arrays.asList(msgs);

			for (final String message : messages) {
				ByteString data = ByteString.copyFromUtf8(message);
				PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

				// Once published, returns a server-assigned message id (unique within the
				// topic)
				ApiFuture<String> future = publisher.publish(pubsubMessage);

				// Add an asynchronous callback to handle success / failure
				ApiFutures.addCallback(future, new ApiFutureCallback<String>() {

					
					public void onFailure(Throwable throwable) {
						if (throwable instanceof ApiException) {
							ApiException apiException = ((ApiException) throwable);
							// details on the API exception
							System.out.println(apiException.getStatusCode().getCode());
							System.out.println(apiException.isRetryable());
						}
						System.out.println("Error publishing message : " + message);
					}

					
					public void onSuccess(String messageId) {
						// Once published, returns server-assigned message ids (unique within the topic)
						System.out.println("Published message ID: " + messageId);
					}
				}, MoreExecutors.directExecutor());
			}
		} catch(Throwable e) {
			
		}
		finally {
			if (publisher == null) {
				return;
			}
			try {
				publisher.shutdown();
				publisher.awaitTermination(1, TimeUnit.MINUTES);
			} catch (Exception e) {
			}
		}
	}

	public void send1(Enum<?> topicName, String... msgs) {
		List<String> asList = Arrays.asList(msgs);
		List<CcpJsonRepresentation> messages = asList.stream().map(message -> this.map(message))
				.collect(Collectors.toList());
		String url = "https://pubsub.googleapis.com/v1/projects/" + PROJECT_ID + "/topics/" + topicName + ":publish";

		CcpAuthenticationProvider authenticationProvider = CcpDependencyInjection
				.getDependency(CcpAuthenticationProvider.class);
		String token = authenticationProvider.getJwtToken();

		CcpJsonRepresentation body = CcpConstants.EMPTY_JSON.put("messages", messages);

		CcpHttpHandler ccpHttpHandler = new CcpHttpHandler(200);
		CcpJsonRepresentation authorization = CcpConstants.EMPTY_JSON.put("Authorization", "Bearer " + token);
		ccpHttpHandler.executeHttpRequest(url, "POST", authorization, body, CcpHttpResponseType.singleRecord);
	}

	private CcpJsonRepresentation map(String message) {
		String value = new CcpStringDecorator(message).text().asBase64();
		CcpJsonRepresentation json = CcpConstants.EMPTY_JSON.put("data", value);
		return json;
	}
}
