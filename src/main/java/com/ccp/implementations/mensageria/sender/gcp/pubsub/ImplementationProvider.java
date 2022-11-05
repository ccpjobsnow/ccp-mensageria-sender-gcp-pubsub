package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import com.ccp.dependency.injection.CcpImplementationProvider;

public class ImplementationProvider implements CcpImplementationProvider {

	@Override
	public Object getImplementation() {
		return new MensageriaSenderGcpPubSub();
	}

}
