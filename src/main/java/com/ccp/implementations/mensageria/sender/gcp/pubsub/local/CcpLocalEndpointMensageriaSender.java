package com.ccp.implementations.mensageria.sender.gcp.pubsub.local;

import com.ccp.dependency.injection.CcpInstanceProvider;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;

public class CcpLocalEndpointMensageriaSender implements CcpInstanceProvider<CcpMensageriaSender> {

	public CcpMensageriaSender getInstance() {
		return new LocalEndpointMensageriaSender();
	}
}
