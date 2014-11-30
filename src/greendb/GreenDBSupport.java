package greendb;

import java.sql.SQLException;
import java.util.List;

import greencode.database.DatabaseConnection;
import greencode.kernel.GreenContext;

public class GreenDBSupport {

	private DatabaseConnection connection = GreenContext.getInstance().getDatabaseConnection();
	private final boolean Synchronize;
	
	public GreenDBSupport() {
		this.Synchronize = false;
	}
	
	public GreenDBSupport(boolean Synchronize) {
		this.Synchronize = Synchronize;
	}
	
	public <E> List<E> findAll(Class<E> model) throws SQLException {
		List<E> list = GreenDB.findAll(connection, model, null);
		if(Synchronize)
			list = GreenDB.synchronizedList(list);
		
		return list;
	}
		
	public <E> List<E> findAll(Class<E> model, String[] fieldNames) throws SQLException {
		List<E> list = GreenDB.findAll(connection, model, fieldNames);
		if(Synchronize)
			list = GreenDB.synchronizedList(list);
		
		return list;
	}
}
