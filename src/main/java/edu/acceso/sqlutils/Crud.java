package edu.acceso.sqlutils;

import java.util.Optional;
import java.util.stream.Stream;

public interface Crud<T extends Entity> {

    public Optional<T> get(int id);
    public Stream<T> get();

    public boolean delete(int id);
    default boolean delete(T obj) {
        return delete(obj.getId());
    }

    public void insert(T obj);
    default void insert(Iterable<T> objs) {
        for(T obj: objs) insert(obj);
    }

    public boolean update(T obj);
    public boolean update(int oldId, int newId);
    default boolean update(T obj, int newId) {
        return update(obj.getId(), newId);
    }
}
