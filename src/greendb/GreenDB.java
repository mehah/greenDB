package greendb;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import greencode.database.DatabasePreparedStatement;
import greencode.database.DatabaseStatement;
import greencode.kernel.Console;
import greencode.kernel.GreenContext;
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
	
	private GreenDB() {
		
	}
	
	private static Field[] getColumns(Class<?> model) {
		return getFields(model, "column$"+model.getName(), fieldsColumns);
	}
	
	private static Field[] getPKs(Class<?> model) {
		return getFields(model, "pk$"+model.getName(), fieldsPK);
	}
	
	private static Field[] getFields(Class<?> model, String ref, Condition<Field> condition) {
		Field[] fields = GenericReflection.getDeclaredFieldsByConditionId(model, ref);
		
		if(fields == null)
			fields = GenericReflection.getDeclaredFieldsByCondition(model, ref, condition, true);
		
		return fields;
	}
	
	public static<E> List<E> findAll(Class<E> model, String[] fieldNames) throws SQLException {
		return findAll(GreenContext.getInstance(), model, fieldNames);
	}
	
	public static<E> List<E> findAll(Class<E> model) throws SQLException {
		return findAll(GreenContext.getInstance(), model, null);
	}
	
	public static<E> List<E> findAll(GreenContext context, Class<E> model) throws SQLException {
		return findAll(context, model, null);
	}
	
	public static<E> List<E> findAll(GreenContext context, Class<E> model, String[] fieldNames) throws SQLException {
		if(!model.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+model.getName());
		
		Field[] fields = getColumns(model);
		
		StringBuilder q = new StringBuilder("SELECT ").append(fieldToColumnNames(fields, fieldNames));
		
		q.append(" FROM ").append(model.getAnnotation(Table.class).value());
		
		DatabaseStatement st = context.getDatabaseConnection().createStatement();
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
	
	private static<E> E buildEntity(ResultSet rs, Class<E> model, Field[] fields, String[] fieldNames) {
		try {
			E instance = model.newInstance();
			boolean hasResult = rs.next();
			if(hasResult) {
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
			Console.error(e);
		}
		
		return null;
	}
	
	public static<E> E findByPK(Class<E> model, String[] selectColumnNames, Object... values) throws SQLException {
		return findByPK(GreenContext.getInstance(), model, selectColumnNames, values);
	}
	
	public static<E> E findByPK(Class<E> model, Object... values) throws SQLException {
		return findByPK(GreenContext.getInstance(), model, null, values);
	}
	
	public static<E> E findByPK(GreenContext context, Class<E> model, Object... values) throws SQLException {
		return findByPK(context, model, null, values);
	}
	
	public static<E> E findByPK(GreenContext context, Class<E> model, String[] selectColumnNames, Object... values) throws SQLException {
		if(!model.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+model.getName());
		
		Field[] fields = getColumns(model);
		
		StringBuilder q = new StringBuilder("SELECT ").append(fieldToColumnNames(fields, selectColumnNames)).append(" FROM ").append(model.getAnnotation(Table.class).value()).append(" WHERE ");
		
		Field[] fieldsPK = getPKs(model);
		
		for (int i = -1; ++i < fieldsPK.length;) {
			Field f = fieldsPK[i];
			if(i > 0)
				q.append(" and ");
			
			Column c = f.getAnnotation(Column.class);
			
			q.append(c.value().isEmpty() ? f.getName() : c.value()).append(" = ?");
		}
		
		
		DatabasePreparedStatement st = context.getDatabaseConnection().prepareStatement(q.toString());
		
		for (int i = -1; ++i < values.length;)
			st.setObject(i+1, values[i]);
				
		return buildEntity(st.executeQuery(), model, fields, null);
	}
	
	public static boolean update(GreenContext context, Object model) throws SQLException {
		Class<?> modelClass = model.getClass();
		if(!modelClass.isAnnotationPresent(Table.class))
			throw new SQLException("Table name not defined in: "+modelClass.getName());
		
		Field[] fieldsPK = getPKs(modelClass);
		if(fieldsPK.length == 0)
			throw new SQLException("To upgrade, need to have primary key in: "+modelClass.getName());
		
		StringBuilder sql = new StringBuilder("UPDATE ").append(modelClass.getAnnotation(Table.class).value()).append(" SET ");
		
		Field[] fields = getColumns(modelClass);
		
		int i = -1;
		for (Field f : fields) {			
			if(f.isAnnotationPresent(PK.class))
				continue;
			
			Column c = f.getAnnotation(Column.class);
			
			if(++i > 0)
				sql.append(",");
			
			sql.append(c.value().isEmpty() ? f.getName() : c.value()).append("=").append("?");
		}
		
		sql.append(" WHERE ");
		
		i = -1;
		for (Field f : fieldsPK) {
			if(++i > 0)
				sql.append(" AND ");
			Column c = f.getAnnotation(Column.class);
			sql.append(c.value().isEmpty() ? f.getName() : c.value()).append("=").append("?");
		}
		
		DatabasePreparedStatement dps = context.getDatabaseConnection().prepareStatement(sql.toString());
		
		i = 0;
		for (Field f : fields) {		
			if(f.isAnnotationPresent(PK.class))
				continue;
			
			dps.setObject(++i, GenericReflection.NoThrow.getValue(f, model));
		}
		
		for (Field f : fieldsPK)
			dps.setObject(++i, GenericReflection.NoThrow.getValue(f, model));
		
		return dps.executeUpdate() > 0;
	}
	
	public static boolean delete(GreenContext context, Object model) throws SQLException {
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
		
		Field[] fieldsPK = getPKs(modelClass);
		if(fieldsPK.length == 0)
			throw new SQLException("To delete, need to have primary key in: "+modelClass.getName());
		
		StringBuilder sql = new StringBuilder("DELETE FROM ").append(modelClass.getAnnotation(Table.class).value()).append(" WHERE ");
		
		int i = -1;		
		final int s = isList ? list.size() : 1;
		
		for (; ++i < s;) {
			if(i > 0)
				sql.append(" OR ");
			
			int i2 = -1;
			for (Field f : fieldsPK) {
				if(++i2 > 0)
					sql.append(" AND ");
				Column c = f.getAnnotation(Column.class);
				sql.append(c.value().isEmpty() ? f.getName() : c.value()).append("=").append("?");
			}
		}
		
		DatabasePreparedStatement dps = context.getDatabaseConnection().prepareStatement(sql.toString());
		
		i = 0;		
		for (int i2 = -1; ++i2 < s;) {
			for (Field f : fieldsPK)
				dps.setObject(++i, GenericReflection.NoThrow.getValue(f, isList ? list.get(i2): model));
		}
		
		return dps.executeUpdate() > 0;
	}
	
	public static boolean insert(GreenContext context, Object model) throws SQLException {
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
		
		Field[] fields = getColumns(modelClass);
		
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
		
		DatabasePreparedStatement dps = context.getDatabaseConnection().prepareStatement(q.toString(), hasAutoIncrementKey ? DatabasePreparedStatement.RETURN_GENERATED_KEYS : DatabasePreparedStatement.NO_GENERATED_KEYS);
		
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
		
		return ok;
	}
	
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
			Console.error(e);
		}
	}
}
