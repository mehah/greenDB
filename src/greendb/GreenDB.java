package greendb;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import greencode.database.DatabaseConnection;
import greencode.database.DatabasePreparedStatement;
import greencode.database.DatabaseStatement;
import greencode.exception.GreencodeError;
import greencode.util.GenericReflection;
import greencode.util.GenericReflection.Condition;
import greendb.annotation.Column;
import greendb.annotation.PK;
import greendb.annotation.Table;

public final class GreenDB {
	private final static Condition<Field> fieldsColumns = new GenericReflection.Condition<Field>() {
		public boolean init(Field f) {					
			return f.isAnnotationPresent(Column.class);
		}
	};
	
	private final static Condition<Field> fieldsPK = new GenericReflection.Condition<Field>() {
		public boolean init(Field f) {					
			return f.isAnnotationPresent(Column.class) && f.isAnnotationPresent(PK.class);
		}
	};
		
	public GreenDB() {}
	
	static Field[] getColumns(Class<?> model, boolean considerParents) {
		return getFields(model, "column$"+model.getName(), fieldsColumns, considerParents);
	}
	
	static Field[] getPKs(Class<?> model) {
		return getFields(model, "pk$"+model.getName(), fieldsPK, true);
	}
	
	static Field[] getPKs(Class<?> model, boolean considerParents) {
		return getFields(model, "pk$"+model.getName(), fieldsPK, considerParents);
	}
	
	private static Field[] getFields(Class<?> model, String ref, Condition<Field> condition, boolean considerParents) {
		Field[] fields = GenericReflection.getDeclaredFieldsByConditionId(model, ref);
		
		if(fields == null)
			fields = GenericReflection.getDeclaredFieldsByCondition(model, ref, condition, considerParents);
		
		return fields;
	}
	
	public static<E> GreenDBList<E> findAllSynchronized(DatabaseConnection connection, Class<E> model) throws SQLException {
		return findAllSynchronized(connection, model, null);
	}
	
	public static<E> GreenDBList<E> findAllSynchronized(DatabaseConnection connection, Class<E> model, String[] fieldNames) throws SQLException {
		return new GreenDBList<E>(findAll(connection, model, fieldNames, null), true);
	}
	
	public static<E> List<E> findAll(DatabaseConnection connection, Class<E> model) throws SQLException {
		return findAll(connection, model, null, null);
	}
	
	public static<E> List<E> findAll(DatabaseConnection connection, Class<E> model, String[] orderByColumnNames) throws SQLException {
		return findAll(connection, model, null, orderByColumnNames);
	}
		
	public static<E> List<E> findAll(DatabaseConnection connection, Class<E> model, String[] fieldNames, String[] orderByColumnNames) throws SQLException {
		if(!model.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+model.getName());
		
		Field[] fields = getColumns(model, true);
		
		StringBuilder q = new StringBuilder("SELECT ").append(fieldToColumnNames(fields, fieldNames));
		
		q.append(" FROM ").append(model.getAnnotation(Table.class).value());
		
		if(orderByColumnNames != null) {
			q.append(" ORDER BY ").append(orderByColumnNames[0]);
			for(int i = 0; ++i < orderByColumnNames.length;) {
				q.append(",").append(orderByColumnNames[i]);
			}
		}
		
		DatabaseStatement st = connection.createStatement();
		ResultSet rs = st.executeQuery(q.toString());		
		
		E o = buildEntity(rs, model, fields, fieldNames);
		if(o == null)		
			return null;
		else {
			List<E> list = new ArrayList<E>();
			list.add(o);
			while((o = buildEntity(rs, model, fields, fieldNames)) != null)
				list.add(o);
			
			return list;
		}
	}
	
	private static StringBuilder fieldToColumnNames(Field[] fields, String[] fieldNames) {
		StringBuilder q = new StringBuilder();
		
		if(fieldNames == null)
			q.append("*");
		else {
			for (int i = -1; ++i < fieldNames.length;) {
				final String name = fieldNames[i];
				for (Field f : fields) {
					if(f.getName().equals(name)) {
						if(i > 0)
							q.append(",");
						Column c = f.getAnnotation(Column.class);
						q.append(c.value().isEmpty() ? name : c.value());
						fieldNames[i] = name;
						break;
					}
				}
			}
		}
		
		return q;
	}
	
	public static<E> E buildObject(ResultSet rs, Class<E> model) {
		return buildEntity(rs, model, getColumns(model, true), null);
	}
	
	public static<E> E buildObject(ResultSet rs, Class<E> model, String[] fieldNames) {
		return buildEntity(rs, model, getColumns(model, true), fieldNames);
	}
		
	private static<E> E buildEntity(ResultSet rs, Class<E> model, Field[] fields, String[] fieldNames) {
		try {
			if(rs.next()) {
				E instance = model.newInstance();
				if(fieldNames == null) {
					for (Field f : fields) {
						Column c = f.getAnnotation(Column.class);
						GenericReflection.NoThrow.setValue(f, rs.getObject(c.value().isEmpty() ? f.getName() : c.value()), instance);
					}
				} else {
					for (String s : fieldNames) {
						for (Field f : fields) {
							if(f.getName().equals(s)) {
								GenericReflection.NoThrow.setValue(f, rs.getObject(s), instance);
								break;
							}
						}
					}
				}
				
				return instance;
			}		
		} catch (Exception e) {
			throw new GreencodeError(e);
		}
		
		return null;
	}
	
	public static<E> E findByPK(DatabaseConnection connection, Class<E> model) throws SQLException {
		return findByPK(connection, model, null);
	}
	public static<E> E findByPK(DatabaseConnection connection, Class<E> model, String[] selectColumnNames, Object... values) throws SQLException {
		if(!model.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+model.getName());
		
		Field[] fields = getColumns(model, true);
		
		StringBuilder q = new StringBuilder("SELECT ").append(fieldToColumnNames(fields, selectColumnNames)).append(" FROM ").append(model.getAnnotation(Table.class).value()).append(" WHERE ");
		
		Field[] fieldsPK = getPKs(model);
		
		for (int i = -1; ++i < fieldsPK.length;) {
			Field f = fieldsPK[i];
			if(i > 0)
				q.append(" and ");
			
			Column c = f.getAnnotation(Column.class);
			
			q.append(c.value().isEmpty() ? f.getName() : c.value()).append(" = ?");
		}
		
		
		DatabasePreparedStatement st = connection.prepareStatement(q.toString());
		
		for (int i = -1; ++i < values.length;)
			st.setObject(i+1, values[i]);
				
		return buildEntity(st.executeQuery(), model, fields, null);
	}
	
	public static<E> List<E> findByColumns(DatabaseConnection connection, Class<E> model, String[] selectColumnNames, String[] whereColumnNames, String[] groupColumnNames, Object... values) throws SQLException {
		if(!model.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+model.getName());
		
		Field[] fields = getColumns(model, true);
		
		StringBuilder q = new StringBuilder("SELECT ").append(fieldToColumnNames(fields, selectColumnNames)).append(" FROM ").append(model.getAnnotation(Table.class).value()).append(" WHERE ");
		
		Field[] fieldsPK = getColumns(model, true);
		
		int i = -1;
		for (Field f: fieldsPK) {			
			Column c = f.getAnnotation(Column.class);
			String name = c.value().isEmpty() ? f.getName() : c.value();
			for(String columnName: whereColumnNames) {
				if(columnName.equals(name)) {
					if(++i > 0)
						q.append(" and ");
					
					q.append(c.value().isEmpty() ? f.getName() : c.value()).append(" = ?");
				}
			}
		}
		
		if(groupColumnNames != null) {
			q.append(" GROUP BY ");
			for(i = -1; ++i < groupColumnNames.length;) {
				if(i > 0)
					q.append(',');
				q.append(groupColumnNames[i]);
			}
		}		
		
		DatabasePreparedStatement st = connection.prepareStatement(q.toString());
		
		for (i = -1; ++i < values.length;)
			st.setObject(i+1, values[i]);
		
		ResultSet rs = st.executeQuery();
		E o = buildEntity(rs, model, fields, selectColumnNames);
		if(o == null)		
			return null;
		else {
			List<E> list = new ArrayList<E>();
			list.add(o);
			while((o = buildEntity(rs, model, fields, selectColumnNames)) != null)
				list.add(o);
			
			return list;
		}
	}
	
	public static boolean update(DatabaseConnection connection, Object model) throws SQLException {
		return update(connection, model, model.getClass());
	}
	
	public static boolean update(DatabaseConnection connection, Object model, /* Temporario */Class<?> ref, String... fieldNames) throws SQLException {
		Class<?> modelClass = ref;
		if(!modelClass.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+modelClass.getName());
		
		Field[] fieldsPK = getPKs(modelClass, false);
		if(fieldsPK.length == 0)
			throw new SQLException("To upgrade, need to have primary key in: "+modelClass.getName());
		
		StringBuilder sql = new StringBuilder("UPDATE ").append(modelClass.getAnnotation(Table.class).value()).append(" SET ");
		
		Field[] fields = getColumns(modelClass, false);
		
		List<String> listFieldNames = Arrays.asList(fieldNames);
		
		int i = -1;
		for (Field f : fields) {
			if(f.isAnnotationPresent(PK.class))
				continue;
			
			Column c = f.getAnnotation(Column.class);
			
			if(!c.updatable())
				continue;
			
			String name = c.value().isEmpty() ? f.getName() : c.value();
			
			if(fieldNames.length > 0 && listFieldNames.indexOf(name) == -1) {
				continue;
			}
			
			if(++i > 0)
				sql.append(",");
			
			sql.append(name).append("=").append("?");
		}
		
		sql.append(" WHERE ");
		
		i = -1;
		for (Field f : fieldsPK) {
			if(++i > 0)
				sql.append(" AND ");
			Column c = f.getAnnotation(Column.class);
			sql.append(c.value().isEmpty() ? f.getName() : c.value()).append("=").append("?");
		}
		
		DatabasePreparedStatement dps = connection.prepareStatement(sql.toString());
		
		i = 0;
		for (Field f : fields) {
			if(f.isAnnotationPresent(PK.class))
				continue;
			
			Column c = f.getAnnotation(Column.class);			
			if(!c.updatable())
				continue;
			
			String name = c.value().isEmpty() ? f.getName() : c.value();
			
			if(fieldNames.length > 0 && listFieldNames.indexOf(name) == -1) {
				continue;
			}
			
			Object value = GenericReflection.NoThrow.getValue(f, model);
			if(value != null && f.getType().equals(Date.class)) {
				dps.setTimestamp(++i, new Timestamp(((Date) value).getTime()));
			} else if(value != null && f.getType().equals(Timestamp.class)) {
				dps.setTimestamp(++i, (Timestamp) value);
			} else {
				dps.setObject(++i, value);
			}
		}
		
		for (Field f : fieldsPK)
			dps.setObject(++i, GenericReflection.NoThrow.getValue(f, model));
		
		return dps.executeUpdate() > 0;
	}
	
	public static boolean delete(DatabaseConnection connection, Object model) throws SQLException {
		return delete(connection, model, null);
	}
	
	public static boolean delete(DatabaseConnection connection, Object model, String[] ignoreFields) throws SQLException {
		Class<?> modelClass;
		final boolean isList = model instanceof List;
		
		@SuppressWarnings("unchecked")
		final List<Object> list = isList ? (List<Object>) model : null;
		if(isList) {
			if(list.size() == 0)
				return false;
			
			modelClass = list.get(0).getClass();
		} else
			modelClass = model.getClass();
		
		if(!modelClass.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+modelClass.getName());
		
		Field[] fieldsCondition = getPKs(modelClass);
		if(fieldsCondition.length == 0)
			fieldsCondition = getColumns(modelClass, true);
		
		StringBuilder sql = new StringBuilder("DELETE FROM ").append(modelClass.getAnnotation(Table.class).value()).append(" WHERE ");
		
		int i = -1;		
		final int s = isList ? list.size() : 1;
		
		List<Object> values = new ArrayList<Object>();
		
		for (; ++i < s;) {
			if(i > 0)
				sql.append(" OR ");
		
			
			int i2 = -1;

			loopFieldsCondition:
			for (Field f : fieldsCondition) {
				Column c = f.getAnnotation(Column.class);
				String fieldName = c.value().isEmpty() ? f.getName() : c.value();
				
				if(ignoreFields != null) {
					for (String _fieldName : ignoreFields) {
						if(fieldName.equals(_fieldName)) {
							continue loopFieldsCondition;
						}
					}
				}
				
				if(++i2 > 0)
					sql.append(" AND ");
				
				Object value = GenericReflection.NoThrow.getValue(f, isList ? list.get(i) : model);
				values.add(value);
				
				
				sql.append(fieldName).append(value == null ? " is " : "=").append("?");
			}
		}
		
		DatabasePreparedStatement dps = connection.prepareStatement(sql.toString());
		
		i = 0;		
		for (Object v : values)
			dps.setObject(++i, v);
		
		return dps.executeUpdate() > 0;
	}
	
	public static boolean insert(DatabaseConnection connection, Object model) throws SQLException {
		return insert(connection, model, null);
	}
	
	public static boolean insert(DatabaseConnection connection, Object model, /* Temporario */Class<?> ref) throws SQLException {
		Class<?> modelClass = ref;
		final boolean isList = model instanceof List;
		
		@SuppressWarnings("unchecked")
		final List<Object> list = isList ? (List<Object>) model : null;
		
		if(ref == null) {
			if(isList) {
				if(list.size() == 0)
					return false;
				
				modelClass = list.get(0).getClass();
			}else {
				modelClass = model.getClass();
			}
		}
		
		if(!modelClass.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+modelClass.getName());
		
		Field[] fields = getColumns(modelClass, false);
		
		StringBuilder q = new StringBuilder("INSERT INTO ").append(modelClass.getAnnotation(Table.class).value()).append("(");
		
		Field fieldWithAutoIncrement = null;
		
		boolean first = true;
		
		for (int i = -1; ++i < fields.length;) {
			Field f = fields[i];
			PK pk = f.getAnnotation(PK.class);
			if(pk != null && pk.autoIncrement()) {
				fieldWithAutoIncrement = f;
				continue;
			}
			
			if(!first)
				q.append(",");
			
			Column c = f.getAnnotation(Column.class);
			q.append(c.value().isEmpty() ? f.getName() : c.value());
			
			first = false;
		}
		q.append(") VALUES");
		
		boolean hasAutoIncrementKey = fieldWithAutoIncrement != null;
		
		int length = fields.length;
		if(hasAutoIncrementKey)
			--length;
		
		if(isList){
			for (int i = -1, s = list.size(); ++i < s;) {
				if(i > 0)
					q.append(",");
				createParamInsertString(q, length);
			}
		} else
			createParamInsertString(q, length);
		
		DatabasePreparedStatement dps = connection.prepareStatement(q.toString(), hasAutoIncrementKey ? DatabasePreparedStatement.RETURN_GENERATED_KEYS : DatabasePreparedStatement.NO_GENERATED_KEYS);
		
		try {
			if(isList) {
				int i = 0;
				for (Object _model : list) {					
					i = setDBObject(fields, fieldWithAutoIncrement, dps, _model, i);
				}
			} else {
				setDBObject(fields, fieldWithAutoIncrement, dps, model, 0);
			}
		} catch (Exception e) {
			throw new GreencodeError(e);
		}		
		
		boolean ok = dps.executeUpdate() > 0;
		
		if(ok && hasAutoIncrementKey) {
			ResultSet rs = dps.getGeneratedKeys();
			
			if(isList) {
				for (Object _model : list) {
					rs.next();
					GenericReflection.NoThrow.setValue(fieldWithAutoIncrement, rs.getInt(1), _model);
				}
			}else {
				rs.next();
				GenericReflection.NoThrow.setValue(fieldWithAutoIncrement, rs.getInt(1), model);
			}	    
		}
		
		return ok;
	}
	
	private static int setDBObject(Field[] fields, Field fieldWithAutoIncrement, DatabasePreparedStatement dps, Object model, int i) throws IllegalArgumentException, SQLException, IllegalAccessException {
		for (Field f : fields) {
			if(f.equals(fieldWithAutoIncrement))
				continue;
			
			if(f.getType().equals(Date.class))
				dps.setTimestamp(++i, new Timestamp(((Date) f.get(model)).getTime()));
			else
				dps.setObject(++i, f.get(model));
		}
		return i;
	}
	
	/*@Deprecated
	public static boolean insert2(DatabaseConnection connection, Object model) throws SQLException {
		Class<?> modelClass;
		final boolean isList = model instanceof List;
		
		@SuppressWarnings("unchecked")
		final List<Object> list = isList ? (List<Object>) model : null;
		if(isList) {
			if(list.size() == 0)
				return false;
			
			modelClass = list.get(0).getClass();
		}else {
			modelClass = model.getClass();
		}
		
		if(!modelClass.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+modelClass.getName());
		
		final Class<?>[] parents = ClassUtils.getParents(modelClass);		
		for(int p = parents.length; --p >= 0;) {
			Class<?> parent = parents[p];
			
			Field[] fields = getColumns(parent, false);
			
			StringBuilder q = new StringBuilder("INSERT INTO ").append(modelClass.getAnnotation(Table.class).value()).append("(");
			
			Field fieldWithAutoIncrement = null;
			
			boolean first = true;
			
			for (int i = -1; ++i < fields.length;) {
				Field f = fields[i];
				PK pk = f.getAnnotation(PK.class);
				if(pk != null && pk.autoIncrement()) {
					fieldWithAutoIncrement = f;
					continue;
				}
				
				if(!first)
					q.append(",");
				
				Column c = f.getAnnotation(Column.class);
				q.append(c.value().isEmpty() ? f.getName() : c.value());
				
				first = false;
			}
			q.append(") VALUES");
			
			boolean hasAutoIncrementKey = fieldWithAutoIncrement != null;
			
			int length = fields.length;
			if(hasAutoIncrementKey)
				--length;
			
			if(isList){
				for (int i = -1, s = list.size(); ++i < s;) {
					if(i > 0)
						q.append(",");
					createParamInsertString(q, length);
				}
			} else
				createParamInsertString(q, length);
			
			DatabasePreparedStatement dps = connection.prepareStatement(q.toString(), hasAutoIncrementKey ? DatabasePreparedStatement.RETURN_GENERATED_KEYS : DatabasePreparedStatement.NO_GENERATED_KEYS);
			
			try {
				if(isList) {
					int i = 0;
					for (Object _model : list) {
						for (Field f : fields) {
							if(f.equals(fieldWithAutoIncrement))
								continue;
							dps.setObject(++i, f.get(_model));
						}
					}
				} else {
					int i = 0;
					for (Field f : fields) {
						if(f.equals(fieldWithAutoIncrement))
							continue;
						dps.setObject(++i, f.get(model));
					}
				}
			} catch (Exception e) {
				Console.error(e);
			}		
			
			boolean ok = dps.executeUpdate() > 0;
			
			if(ok && hasAutoIncrementKey) {
				ResultSet rs = dps.getGeneratedKeys();
				
				if(isList) {
					for (Object _model : list) {
						rs.next();
						GenericReflection.NoThrow.setValue(fieldWithAutoIncrement, rs.getInt(1), _model);
					}
				}else {
					rs.next();
					GenericReflection.NoThrow.setValue(fieldWithAutoIncrement, rs.getInt(1), model);
				}	    
			}
		}
		
		return ok;
	}*/
	
	private static void createParamInsertString(StringBuilder q, final int length) {
		try {
			q.append("(");
			for (int i = -1; ++i < length;) {
				if(i > 0)
					q.append(",");
				
				q.append("?");
			}
			q.append(")");
		} catch (Exception e) {
			throw new GreencodeError(e);
		}
	}
	
	public static<E> GreenDBList<E> synchronizedList(List<E> list) {
		return GreenDBList.Synchronized(list);
	}
}
