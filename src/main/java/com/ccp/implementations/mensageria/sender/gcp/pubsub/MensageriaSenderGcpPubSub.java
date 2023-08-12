package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.main.authentication.CcpAuthenticationProvider;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.google.cloud.ServiceOptions;

class MensageriaSenderGcpPubSub implements CcpMensageriaSender {

	@CcpDependencyInject
	private CcpHttpRequester ccpHttp;

	@CcpDependencyInject
	private CcpAuthenticationProvider authenticationProvider;

	public void send(Enum<?> topicName , String... msgs) {
		String projectId = ServiceOptions.getDefaultProjectId();
		List<String> asList = Arrays.asList(msgs);
		List<CcpMapDecorator> messages = asList.stream().map(message -> this.map(message)).collect(Collectors.toList());
		String url = "https://pubsub.googleapis.com/v1/projects/"
				+ projectId
				+ "/topics/"
				+ topicName
				+ ":publish";
		
		String token = this.authenticationProvider.getJwtToken();
		
		CcpMapDecorator put = new CcpMapDecorator().put("messages", messages);
		
		this.ccpHttp.executeHttpRequest(url, "POST", new CcpMapDecorator().put("Authorization", "Bearer " + token), put.asJson());
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
