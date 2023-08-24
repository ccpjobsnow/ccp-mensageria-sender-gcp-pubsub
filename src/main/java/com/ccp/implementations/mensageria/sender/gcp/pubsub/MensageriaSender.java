package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class MensageriaSender implements CcpInstanceProvider {

	@Override
	public Object getInstance() {
		return new MensageriaSenderGcpPubSub();
	}

}
