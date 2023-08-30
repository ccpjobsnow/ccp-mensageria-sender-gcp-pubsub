package com.ccp.implementations.mensageria.sender.gcp.pubsub.local;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class LocalMensageriaSender implements CcpInstanceProvider {

	@Override
	public Object getInstance() {
		return new MensageriaSenderGcpPubSubLocal();
	}

}
