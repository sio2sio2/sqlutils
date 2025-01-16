package edu.acceso.sqlutils;

import java.util.Optional;
import java.util.stream.Stream;

public interface Crud<T extends Entity> {

    public Optional<T> get(int id) throws DataAccessException;
    public Stream<T> get() throws DataAccessException;

    public boolean delete(int id) throws DataAccessException;
    default boolean delete(T obj) throws DataAccessException {
        return delete(obj.getId());
    }

    public void insert(T obj) throws DataAccessException;
    default void insert(Iterable<T> objs) throws DataAccessException {
        for(T obj: objs) insert(obj);
    }

    public boolean update(T obj) throws DataAccessException;
    public boolean update(int oldId, int newId) throws DataAccessException;
    default boolean update(T obj, int newId) throws DataAccessException {
        return update(obj.getId(), newId);
    }
}
