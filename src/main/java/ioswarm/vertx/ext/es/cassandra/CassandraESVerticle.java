package ioswarm.vertx.ext.es.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;

import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ioswarm.vertx.ext.cassandra.CassandraClient;
import ioswarm.vertx.ext.es.AbstractESVerticle;

public abstract class CassandraESVerticle<T> extends AbstractESVerticle<T> {
	
	@Override
	public void start() throws Exception {
		super.start();
		final JsonObject opt = new JsonObject().put("keyspace", "ioswarm");
		final CassandraClient client = CassandraClient.createShared(vertx, opt, scope());
		vertx.executeBlocking(handler -> {
			client.session().execute(String.format(Statements.CREATE_TABLE, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()));
			handler.complete();
		}, res -> {
			client.query(String.format(Statements.SELECT_EVENTS, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()), new JsonArray().add(id()), result -> {
				if (result.succeeded()) {
					for (JsonObject o : result.result())
						System.out.println(o.encode());
				} else result.cause().printStackTrace();
			});
		});

	}

}
