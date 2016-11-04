package examples;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import ioswarm.vertx.ext.es.Event;
import ioswarm.vertx.ext.es.EventHandler;
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
	public JsonObject unmarshall(byte[] data) {
		JsonObject o = new JsonObject();
		o.readFromBuffer(0, Buffer.buffer(data));
		return o;
	}
	
	@Override
	public byte[] marshall(JsonObject o) {
		if (o == null) return new byte[0];
		Buffer b = Buffer.buffer();
		o.writeToBuffer(b);
		return b.getBytes();
	}

	@Override
	public void recover(Event<JsonObject> t) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void snapshot() throws Exception {
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
		
		Thread.sleep(5000l);
		
		EventHandler<JsonObject> handler = new EventHandler<JsonObject>(vertx, "vehicle");
		handler.send("insert", "TEST", new JsonObject().put("message", "ping TEST"));
		
	}
	
}
