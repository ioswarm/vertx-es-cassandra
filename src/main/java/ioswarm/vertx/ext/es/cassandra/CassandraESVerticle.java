package ioswarm.vertx.ext.es.cassandra;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import ioswarm.vertx.ext.cassandra.CassandraClient;
import ioswarm.vertx.ext.es.AbstractESVerticle;
import ioswarm.vertx.ext.es.Event;

public abstract class CassandraESVerticle<T> extends AbstractESVerticle<T> {
	
	protected CassandraClient client;
	
	protected Event<T> createEvent(JsonObject o) throws IOException {
		return new Event<T>(o.getString("id"), o.getInstant("event_date"), o.getString("command"), unmarshall(o.getBinary("content")));
	}
	
	public JsonObject config() {
		JsonObject cfg = new JsonObject().put("keyspace", "ioswarm").put("host", "localhost");
		Config conf = ConfigFactory.load().getConfig("ioswarm.eventsourcing.cassandra");
		if (conf.hasPath("hosts")) {
			JsonArray hosts = new JsonArray();
			for (String h : conf.getString("hosts").split(","))
				hosts.add(h);
			cfg.put("hosts", hosts);
		} else if (conf.hasPath("host")) cfg.put("host", conf.getString("host"));
		if (conf.hasPath("keyspace")) cfg.put("keyspace", conf.getString("keyspace"));
		return  cfg;
	}
	
//	public CassandraClient client() { return client(config()); }
//	public CassandraClient client(JsonObject config) { return CassandraClient.createShared(vertx, config, scope()); }
	public CassandraClient createClient(JsonObject config) {
		return CassandraClient.createShared(vertx, config, scope());
	}
	
	@Override
	public void start(Future<Void> startFuture) throws Exception {
		super.start();
		final JsonObject opt = config();
		
		vertx.executeBlocking(handler -> {
			client = createClient(opt);
			client.session().execute(String.format(Statements.CREATE_TABLE, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()));
			handler.complete();
		}, res -> {
//			for (JsonObject o : client.query(String.format(Statements.SELECT_EVENTS, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()), new JsonArray().add(id()))) {
//				try {
//					info("call recovery");
//					recover(createEvent(o));
//				} catch(Exception e) {
//					e.printStackTrace();
//				}
//			}
			if (res.succeeded())
				client.query(String.format(Statements.SELECT_EVENTS, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()), new JsonArray().add(id()), result -> {
					if (result.succeeded()) {
						info("call recovery id: "+scope()+"."+id()+" for "+result.result().size()+" events.");
						for (JsonObject o : result.result()) {
							try {
								recover(createEvent(o));
							} catch(Exception e) {
								e.printStackTrace();
							}
						}
						startFuture.complete();
					} else startFuture.fail(result.cause()); //result.cause().printStackTrace(); // TODO log
				});
			else 
				error("Error while initial CassandraES "+getClass().getName()+".", res.cause());
		});
//		client.session().execute(String.format(Statements.CREATE_TABLE, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()));
//		for (JsonObject o : client.query(String.format(Statements.SELECT_EVENTS, opt.getString("keyspace", "ioswarm"), scope().toUpperCase()), new JsonArray().add(id()))) {
//			try {
//				info("call recovery");
//				recover(createEvent(o));
//			} catch(Exception e) {
//				e.printStackTrace();
//			}
//		}
	}
	
	public void stop() throws Exception {
		if (client != null) 
			client.close();
		super.stop();
	}
	
	@Override
	public void recover() {
		
	}
	
	@Override
	public Event<T> persist(Event<T> evt) throws Exception {
		final JsonObject opt = config();
		
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
