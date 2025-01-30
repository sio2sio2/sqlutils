package edu.acceso.sqlutils.dao;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;
import edu.acceso.sqlutils.Entity;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.errors.DataAccessRuntimeException;

public class Dao {

    private final Map<Class<? extends Entity>, Crud<? extends Entity>> daos = new HashMap<>();

    @SuppressWarnings("unchecked")
    private static Class<? extends Entity> getEntityType(Class<? extends Crud<? extends Entity>> daoClass) {
        for(Type genericInterface: daoClass.getGenericInterfaces()) {
            if (genericInterface instanceof ParameterizedType) {
                if (((ParameterizedType) genericInterface).getRawType().equals(Crud.class)) {
                    Type[] typeArguments = ((ParameterizedType) genericInterface).getActualTypeArguments();
                    if (typeArguments.length > 0 && typeArguments[0] instanceof Class<?> typeArg && Entity.class.isAssignableFrom(typeArg)) {
                        return (Class<? extends Entity>) typeArg; // Devuelve la clase del tipo genérico
                    }
                }
            }
        }
        throw new IllegalArgumentException(String.format("La clase '%s' no implementa Crud<T> con un tipo genérico específico.", daoClass.getName()));
    }

    private static Constructor<?> selectConstructor(Class<?> clazz, Class<?> argumentClass) throws NoSuchMethodException {
        try {
            return clazz.getConstructor(argumentClass);
        }
        catch(NoSuchMethodException err) {
            for(var ctor: clazz.getConstructors()) {
                Class<?>[] argumentTypes = ctor.getParameterTypes();
                if(argumentTypes.length != 1) continue;
                if(argumentTypes[0].isAssignableFrom(argumentClass)) return ctor;
            }
            throw new NoSuchMethodException(String.format("La clase '%s' no implementa un constructor que admita un objeto %s", clazz, argumentClass.getSimpleName()));
        }
    }

    /**
     * Crea un objeto a partir de la clase proporcionada.
     * @param daoClass La clase DAO de la que se quiere instanciar el objeto
     * @param source El objeto que necesita el constructor.
     */
    @SuppressWarnings("unchecked")
    private void init(Object source, Class<? extends Crud<? extends Entity>>[] daoClasses) {
        for(var daoClass: daoClasses) {
            if(!AbstractDao.class.isAssignableFrom(daoClass)) {
                throw new DataAccessRuntimeException(String.format("La clase '%s' debe extender AbstractDao", daoClass.getName()));
            }
            try {
                var ctor = (Constructor<? extends Crud<? extends Entity>>) selectConstructor(daoClass, source.getClass());
                daos.put(getEntityType(daoClass), ctor.newInstance(source));
            } catch(NoSuchMethodException err) {
                throw new DataAccessRuntimeException(String.format("La clase '%s' no implementa un constructor que admita un objeto %s", daoClass.getName(), source.getClass().getSimpleName()), err);
            } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                    | SecurityException err) {
                throw new DataAccessRuntimeException(String.format("No se pudo crear una instancia de la clase '%s'", daoClass.getName()), err);
            }
        }
    }

    public Dao(DataSource ds, Class<? extends Crud<? extends Entity>>[] daoClasses) {
        init(ds, daoClasses);
    }

    public Dao(Connection conn, Class<? extends Crud<? extends Entity>>[] daoClasses) {
        init(conn, daoClasses);
    }

    public Dao(ConnectionProvider cp, Class<? extends Crud<? extends Entity>>[] daoClasses) {
        init(cp, daoClasses);
    }

    @SuppressWarnings("unchecked")
    public <T extends Entity> Crud<T> getClassDao(Class<T> clazz) {
        Crud<?> dao = daos.get(clazz);
        if(dao == null) {  // Quizás clazz es una subclase y también vale.
            for(Class<? extends Entity> entityClass: daos.keySet()) {
                if(entityClass.isAssignableFrom(clazz)) {
                    dao = daos.get(entityClass);
                    continue;
                }
            }
        }
        return (Crud<T>) dao;
    }

    public <T extends Entity> Optional<T> get(Class<T> clazz, int id) throws DataAccessException {
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return (Optional<T>) dao.get(id);
    }

    public <T extends Entity> Stream<T> get(Class<T> clazz) throws DataAccessException {
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return dao.get();
    }

    public <T extends Entity> boolean delete(Class<T> clazz, int id) throws DataAccessException {
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return dao.delete(id);
    }

    public <T extends Entity> boolean delete(T object) throws DataAccessException {
        return delete(object.getClass(), object.getId());
    }

    @SuppressWarnings("unchecked")
    public <T extends Entity> void insert(T object) throws DataAccessException {
        Class<T> clazz = (Class<T>) object.getClass();
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        dao.insert(object);
    }

    public <T extends Entity> void insert(Class<T> clazz, Iterable<T> objs) throws DataAccessException {
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        dao.insert(objs);
    }

    @SuppressWarnings("unchecked")
    public <T extends Entity> void insert(T[] objs) throws DataAccessException {
        if(objs.length == 0) return;

        Class<T> clazz = (Class<T>) objs[0].getClass();
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        dao.insert(objs);
    }

    @SuppressWarnings("unchecked")
    public <T extends Entity> boolean update(T object) throws DataAccessException {
        Class<T> clazz = (Class<T>) object.getClass();
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return ((Crud<T>) dao).update(object);
    }

    public <T extends Entity> boolean update(Class<T> clazz, int oldId, int newId) throws DataAccessException {
        Crud<T> dao = getClassDao(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return dao.update(oldId, newId);
    }

    public <T extends Entity> boolean update(T object, int newId) throws DataAccessException {
        return update(object.getClass(), object.getId(), newId);
    }
}
