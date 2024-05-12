package com.ccp.implementations.mensageria.sender.gcp.pubsub.local;

import com.ccp.decorators.CcpJsonRepresentation;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.mensageria.sender.CcpMensageriaSender;
import com.ccp.exceptions.process.CcpAsyncTask;
import com.ccp.implementations.db.bulk.elasticsearch.CcpElasticSerchDbBulk;
import com.ccp.implementations.db.crud.elasticsearch.CcpElasticSearchCrud;
import com.ccp.implementations.db.query.elasticsearch.CcpElasticSearchQueryExecutor;
import com.ccp.implementations.db.utils.elasticsearch.CcpElasticSearchDbRequest;
import com.ccp.implementations.email.sendgrid.CcpSendGridEmailSender;
import com.ccp.implementations.file.bucket.gcp.CcpGcpFileBucket;
import com.ccp.implementations.http.apache.mime.CcpApacheMimeHttp;
import com.ccp.implementations.instant.messenger.telegram.CcpTelegramInstantMessenger;
import com.ccp.implementations.json.gson.CcpGsonJsonHandler;
import com.ccp.jn.async.business.factory.CcpJnAsyncBusinessFactory;
import com.ccp.jn.async.business.support.JnAsyncBusinessNotifyError;
import com.jn.commons.entities.JnEntityAsyncTask;

public class LocalMensageriaSender  implements CcpMensageriaSender {

	public LocalMensageriaSender() {
		CcpDependencyInjection.loadAllDependencies(
				new CcpElasticSearchQueryExecutor(),
				new CcpTelegramInstantMessenger(),
				new CcpJnAsyncBusinessFactory(),
				new CcpElasticSearchDbRequest(),
				new CcpSendGridEmailSender(),
				new CcpElasticSerchDbBulk(),
				new CcpElasticSearchCrud(),
				new CcpGsonJsonHandler(),
				new CcpApacheMimeHttp(),
				new CcpGcpFileBucket()  
			
	);

	}
	
	public void send(String topic, String... msgs) {
		
		for (String msg : msgs) {
			new Thread(() -> this.send(topic, msg)).start();
		}
	}

	
	private void send(String topic, String msg) {
		CcpJsonRepresentation md = new CcpJsonRepresentation(msg);
		CcpAsyncTask.executeProcess(topic, md, JnEntityAsyncTask.INSTANCE, JnAsyncBusinessNotifyError.INSTANCE);
	}
}
