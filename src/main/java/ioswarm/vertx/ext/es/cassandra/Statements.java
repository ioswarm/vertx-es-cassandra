package ioswarm.vertx.ext.es.cassandra;

public class Statements {

	public static final String CREATE_TABLE = 
			"CREATE TABLE IF NOT EXISTS %1$s.%2$s ("
			+ "id text, "
			+ "event_date timestamp, "
			+ "command text, "
			+ "content blob, "
			+ "PRIMARY KEY (id, event_date, command) "
			+ ")";
	
	
	public static final String SELECT_EVENTS = 
			"SELECT id, event_date, command, content from %1$s.%2$s "
			+ "WHERE id = ? "
			+ "ORDER BY event_date ASC";
	
	
}
