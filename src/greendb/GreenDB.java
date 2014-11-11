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

}