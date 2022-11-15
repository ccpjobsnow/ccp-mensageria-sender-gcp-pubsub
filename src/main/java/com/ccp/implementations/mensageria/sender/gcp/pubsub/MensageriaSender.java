package com.ccp.implementations.mensageria.sender.gcp.pubsub;

import com.ccp.dependency.injection.CcpModuleExporter;

public class MensageriaSender implements CcpModuleExporter {

	@Override
	public Object export() {
		return new MensageriaSenderGcpPubSub();
	}

}
