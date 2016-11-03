package examples;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import ioswarm.vertx.ext.es.cassandra.CassandraESVerticle;

public class Example extends CassandraESVerticle<JsonObject> {

	@Override
	public String scope() { return "vehicle"; }
	
	@Override
	public String id() {
		return "TEST";
	}

	@Override
	public JsonObject persist(JsonObject t) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void recover(JsonObject t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void snapshot(JsonObject t) throws Exception {
		// TODO Auto-generated method stub
		
	}

	public static void main(String[] args) throws Exception {
		Vertx vertx = Vertx.vertx();
		
		vertx.deployVerticle(new Example());
	}
	
}
