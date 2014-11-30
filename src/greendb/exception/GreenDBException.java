package greendb.exception;

import java.sql.SQLException;

public class GreenDBException extends RuntimeException {
	private static final long serialVersionUID = -6760478253509665854L;
	public GreenDBException() {}
	public GreenDBException(String message) { super(message); }
	public GreenDBException(SQLException e) { super(e); }
}
