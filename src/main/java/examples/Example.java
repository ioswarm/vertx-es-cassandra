package examples;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import ioswarm.vertx.ext.es.cassandra.CassandraESVerticle;

public class Example extends CassandraESVerticle<JsonObject> {

	private final String id;
	
	public Example(String id) {
		this.id = id;
	}
	
	@Override
	public String scope() { return "vehicle"; }
	
	@Override
	public String id() {
		return id;
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
		
		long start = System.currentTimeMillis();
		vertx.deployVerticle(new Example("TEST"));
		System.out.println("TEST started in "+(System.currentTimeMillis()-start)+" ms");
		
		start = System.currentTimeMillis();
		vertx.deployVerticle(new Example("TEST2"));
		System.out.println("TEST2 started in "+(System.currentTimeMillis()-start)+" ms");
	}
	
}
