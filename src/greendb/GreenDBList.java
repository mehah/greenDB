package greendb;

import greencode.database.DatabaseConnection;
import greencode.kernel.GreenContext;
import greendb.exception.GreenDBException;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public final class GreenDBList<E> implements List<E> {	
	
	private DatabaseConnection connection;
	private final List<E> list;
	
	private DatabaseConnection getConnection() {
		try {
			if(connection == null || connection.isClosed())
				this.connection = GreenContext.getInstance().getDatabaseConnection();
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		
		return this.connection;
	}
	
	private void insertList(Collection<?> list) {
		if(!(list instanceof GreenDBList)) {
			try {
				GreenDB.insert(getConnection(), list);
			} catch (SQLException e) {
				throw new GreenDBException(e);
			}
		}
	}
	
	public GreenDBList() {
		this.list = new ArrayList<E>();
	}
	
	public GreenDBList(int size) {
		this.list = new ArrayList<E>(size);
	}
	
	public GreenDBList(List<E> list) {
		this(list, false);
	}
	
	GreenDBList(List<E> list, boolean isAlreadySynchronized) {
		this.list = list;
		if(!isAlreadySynchronized)
			insertList(list);
	}
	
	public static<E> GreenDBList<E> Synchronized(List<E> list) {
		return new GreenDBList<E>(list, true);
	}
	
	public boolean add(E arg0) {
		try {
			GreenDB.insert(getConnection(), arg0);
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		return list.add(arg0);
	}

	public void add(int arg0, E arg1) {
		try {
			GreenDB.insert(getConnection(), arg0);
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		list.add(arg0, arg1);
	}

	public boolean addAll(Collection<? extends E> arg0) {
		insertList(arg0);
		return list.addAll(arg0);
	}

	public boolean addAll(int arg0, Collection<? extends E> arg1) {
		insertList(arg1);
		return list.addAll(arg0, arg1);
	}

	public void clear() {
		try {
			GreenDB.delete(getConnection(), list);
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		list.clear();
	}

	public boolean remove(Object arg0) {
		try {
			GreenDB.delete(getConnection(), arg0);
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		return list.remove(arg0);
	}

	public E remove(int arg0) {
		try {
			GreenDB.delete(getConnection(), list.get(arg0));
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		return list.remove(arg0);
	}

	public boolean removeAll(Collection<?> arg0) {
		try {
			GreenDB.delete(getConnection(), arg0);
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		return list.removeAll(arg0);
	}

	public boolean retainAll(Collection<?> arg0) {
		ArrayList<E> subList = new ArrayList<E>();
		for (E o : list) {
			if(!arg0.contains(o))
				subList.add(o);
		}
		
		if(subList.isEmpty())
			return false;
		
		try {
			GreenDB.delete(getConnection(), subList);
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		
		return list.retainAll(arg0);
	}

	public E set(int arg0, E arg1) {
		try {
			E o = list.get(arg0);
			GreenDB.delete(getConnection(), o);
			GreenDB.insert(getConnection(), arg1);
		} catch (SQLException e) {
			throw new GreenDBException(e);
		}
		
		return list.set(arg0, arg1);
	}

	public int size() {
		return list.size();
	}

	public List<E> subList(int arg0, int arg1) {
		return new GreenDBList<E>(list.subList(arg0, arg1));
	}

	public Object[] toArray() {
		return list.toArray();
	}

	public <T> T[] toArray(T[] arg0) {
		return list.toArray(arg0);
	}

	public boolean contains(Object arg0) {
		return list.contains(arg0);
	}

	public boolean containsAll(Collection<?> arg0) {
		return list.containsAll(arg0);
	}

	public E get(int arg0) {
		return list.get(arg0);
	}

	public int indexOf(Object arg0) {
		return list.indexOf(arg0);
	}

	public boolean isEmpty() {
		return list.isEmpty();
	}

	public Iterator<E> iterator() {
		return list.iterator();
	}

	public int lastIndexOf(Object arg0) {
		return list.lastIndexOf(arg0);
	}

	public ListIterator<E> listIterator() {
		return list.listIterator();
	}

	public ListIterator<E> listIterator(int arg0) {
		return list.listIterator(arg0);
	}

}
