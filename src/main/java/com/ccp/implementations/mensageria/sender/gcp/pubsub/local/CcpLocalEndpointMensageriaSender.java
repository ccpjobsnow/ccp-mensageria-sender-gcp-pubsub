package com.ccp.implementations.mensageria.sender.gcp.pubsub.local;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class CcpLocalEndpointMensageriaSender implements CcpInstanceProvider {

	@Override
	public Object getInstance() {
		return new LocalEndpointMensageriaSender();
	}

}
