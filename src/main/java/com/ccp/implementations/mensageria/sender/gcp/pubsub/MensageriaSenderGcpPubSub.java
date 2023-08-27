package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.http.CcpHttpHandler;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.especifications.main.authentication.CcpAuthenticationProvider;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;

class MensageriaSenderGcpPubSub implements CcpMensageriaSender {


	public void send(Enum<?> topicName , String... msgs) {
//		String projectId = ServiceOptions.getDefaultProjectId();
		String projectId = "jn-hmg";
		List<String> asList = Arrays.asList(msgs);
		List<CcpMapDecorator> messages = asList.stream().map(message -> this.map(message)).collect(Collectors.toList());
		String url = "https://pubsub.googleapis.com/v1/projects/"
				+ projectId
				+ "/topics/"
				+ topicName
				+ ":publish";
		
		CcpAuthenticationProvider authenticationProvider = CcpDependencyInjection.getDependency(CcpAuthenticationProvider.class);
		String token = authenticationProvider.getJwtToken();
		
		CcpMapDecorator body = new CcpMapDecorator().put("messages", messages);
		
		CcpHttpHandler ccpHttpHandler = new CcpHttpHandler(200);
		CcpMapDecorator authorization = new CcpMapDecorator().put("Authorization", "Bearer " + token);
		CcpMapDecorator response = ccpHttpHandler.executeHttpRequest(url, "POST", authorization, body, CcpHttpResponseType.singleRecord);
		System.out.println(response);
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
