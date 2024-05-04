package com.ccp.implementations.mensageria.sender.gcp.pubsub.local;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.ccp.exceptions.process.CcpAsyncTask;
import com.ccp.jn.async.business.support.JnAsyncBusinessNotifyError;

public class LocalMensageriaSender  implements CcpMensageriaSender {

	
	public void send(String topic, String... msgs) {
		
		for (String msg : msgs) {
			new Thread(() -> this.send(topic, msg)).start();
		}
	}

	
	private void send(String topic, String msg) {
		CcpJsonRepresentation md = new CcpJsonRepresentation(msg);
		CcpAsyncTask.executeProcess(topic, md, JnAsyncBusinessNotifyError.INSTANCE);
	}
}
