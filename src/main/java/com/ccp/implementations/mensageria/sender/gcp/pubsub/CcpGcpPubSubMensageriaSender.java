package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class CcpGcpPubSubMensageriaSender implements CcpInstanceProvider {

	
	public Object getInstance() {
		return new GcpPubSubMensageriaSender();
	}

}
