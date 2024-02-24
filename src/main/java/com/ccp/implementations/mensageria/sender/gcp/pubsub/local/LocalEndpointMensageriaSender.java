package com.ccp.implementations.mensageria.sender.gcp.pubsub.local;

import com.ccp.constantes.CcpConstants;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.ccp.especifications.mensageria.sender.CcpTopic;

public class LocalEndpointMensageriaSender  implements CcpMensageriaSender {

	
	public void send(CcpTopic topic, String... msgs) {
		
		for (String msg : msgs) {
			new Thread(() -> this.send(topic, msg)).start();
		}
	}

	
	private void send(CcpTopic topic, String msg) {
		CcpHttpRequester dependency = CcpDependencyInjection.getDependency(CcpHttpRequester.class);
		String url = "http://localhost:8081/" + topic + "/testing";
		dependency.executeHttpRequest(url, "POST", CcpConstants.EMPTY_JSON, msg, 200);
	}
}
