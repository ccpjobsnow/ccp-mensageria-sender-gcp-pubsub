package com.ccp.implementations.mensageria.sender.gcp.pubsub.local;

import com.ccp.constantes.CcpConstants;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.http.CcpHttpRequester;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;

public class LocalEndpointMensageriaSender  implements CcpMensageriaSender {

	@Override
	public void send(Enum<?> topic, String... msgs) {
		
		for (String msg : msgs) {
			new Thread(() -> this.send(topic, msg)).start();
		}
	}

	
	private void send(Enum<?> topic, String msg) {
		CcpHttpRequester dependency = CcpDependencyInjection.getDependency(CcpHttpRequester.class);
		dependency.executeHttpRequest("http://localhost:8081/" + topic.name() + "/testing", "POST", 
				CcpConstants.EMPTY_JSON, 
				msg, 200);
	}
}
