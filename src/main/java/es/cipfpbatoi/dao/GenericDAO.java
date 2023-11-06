package es.cipfpbatoi.dao;
import java.util.List;

public interface GenericDAO<T> {
	T find(int id) throws Exception;

	List<T> findAll() throws Exception;

	T insert(T t) throws Exception;

	boolean update(T t) throws Exception;
	
	boolean save(T t) throws Exception;

	 boolean delete(int id) throws Exception;

	boolean delete(T t) throws Exception;

	long size() throws Exception;

	List<T> findByExample(T t) throws Exception;
}

