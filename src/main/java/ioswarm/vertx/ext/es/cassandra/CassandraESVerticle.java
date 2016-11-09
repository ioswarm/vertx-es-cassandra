package ioswarm.vertx.ext.es.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ioswarm.vertx.ext.cassandra.CassandraClient;
import ioswarm.vertx.ext.es.AbstractESVerticle;
import ioswarm.vertx.ext.es.Event;

public abstract class CassandraESVerticle<T> extends AbstractESVerticle<T> {
	
	protected Event<T> createEvent(JsonObject o) throws IOException {
		return new Event<T>(o.getString("id"), o.getInstant("event_date"), o.getString("command"), unmarshall(o.getBinary("content")));
	}
	
	public JsonObject config() { return new JsonObject().put("keyspace", "ioswarm"); } // TODO implement 
	
	public CassandraClient client() { return client(config()); }
	public CassandraClient client(JsonObject config) { return CassandraClient.createShared(vertx, config, scope()); }
	
	@Override
	public void start() throws Exception {
		super.start();
		final JsonObject opt = config();
		final CassandraClient client = client(opt);
		vertx.executeBlocking(handler -> {
			client.session().execute(String.format(Statements.CREATE_TABLE, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()));
			handler.complete();
		}, res -> {
			client.query(String.format(Statements.SELECT_EVENTS, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()), new JsonArray().add(id()), result -> {
				if (result.succeeded()) {
					for (JsonObject o : result.result()) {
						try {
							info("call recovery");
							recover(createEvent(o));
						} catch(Exception e) {
							e.printStackTrace();
						}
					}
				} else result.cause().printStackTrace();
			});
		});

	}
	
	@Override
	public Event<T> persist(Event<T> evt) throws Exception {
		final JsonObject opt = config();
		final CassandraClient client = client(opt);
		
		PreparedStatement pstmt = client.session().prepare(String.format(Statements.INSERT_EVENT, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()));
		BoundStatement bstmt = pstmt.bind();
		bstmt.setString("id", evt.getId())
			.setTimestamp("event_date", Date.from(evt.getEventDate()))
			.setString("command", evt.getCommand())
			.setBytes("content", ByteBuffer.wrap(marshall(evt.getContent())));
		
		client.session().execute(bstmt);
		
		return evt;
	}
	

}
