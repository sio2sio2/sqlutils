package edu.acceso.sqlutils.dao;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;
import edu.acceso.sqlutils.Entity;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.errors.DataAccessRuntimeException;

/**
 * Modela el objeto que permite gestionar en uno solo todos los objetos
 * DAO que se encargan de hacer persistentes los objetos en una base de datos.
 * Presenta los mismos métodos que la interfaz {@link Crud}, con la salvedad
 * de añadir la clase del objeto sobre el que se desea hacer la operación, si
 * esta no puede colegirse de los argumentos. Por ejemplo, `.delete(T obj)`
 * podrá tener ese único argumento; pero `.delete(int id)` debe especificar
 * la clase: `.delete(Class<T> clazz, int id)`.
 */
public class Dao {

    /**
     * Mapa que relaciona cada clase de las entidades con el objeto DAO que es capaz
     * de hacerla persistente.
     */
    private final Map<Class<? extends Entity>, Crud<? extends Entity>> daos = new HashMap<>();

    /**
     * Obtiene la clase de la entidad a partir de la clase del objeto DAO que la hace persistente.
     * @param daoClass La clase DAO.
     * @return La clase de la entidad para la que se definió la clase DAO.
     */
    @SuppressWarnings("unchecked")
    static Class<? extends Entity> getEntityType(Class<? extends Crud<? extends Entity>> daoClass) {
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

    /**
     * Obtiene el constructor que requiere una determina clase de argumento. El constructor puede estar
     * definido para una superclase de la clase proporcionada.
     * @param clazz La clase de la que se quiere determinar el constructor.
     * @param argumentClass La clase del argumento que se pasa al constructor.
     * @return El constructor apropiado.
     * @throws NoSuchMethodException Cuando no existe constructor que acepte la clase del argumento.
     */
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

    /**
     * Constructor de la clase.
     * @param ds Pool de conexiones que se usará para hacer las operaciones CRUD. En este
     * caso, cada operación debería crear una nueva conexión.
     * @param daoClasses Lista de clases DAO que permiten la persistencia de los objetos.
     */
    public Dao(DataSource ds, Class<? extends Crud<? extends Entity>>[] daoClasses) {
        init(ds, daoClasses);
    }

    /**
     * Constructor de la clase.
     * @param conn Conexión con el que se hará todas las operaciones CRUD hechas con este objeto.
     * @param daoClasses Lista de clases DAO que permiten la persistencia de los objetos.
     */
    public Dao(Connection conn, Class<? extends Crud<? extends Entity>>[] daoClasses) {
        init(conn, daoClasses);
    }

    /**
     * Constructor de la clase.
     * @param cp Proveedor de conexiones usado para realizar las operaciones CRUD. Útil cuando
     *  se crea el objeto dentro de una operación CRUD.
     * @param daoClasses Lista de clases DAO que permiten la persistencia de los objetos.
     */
    public Dao(ConnectionProvider cp, Class<? extends Crud<? extends Entity>>[] daoClasses) {
        init(cp, daoClasses);
    }

    /**
     * Obtiene la clase DAO que hace persistente la clase de objetos pasada como argumento.
     * @param <T> 
     * @param clazz Clase de la entidad.
     * @return La clase DAO apropiada.
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> Crud<T> getDaoClass(Class<T> clazz) {
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
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return (Optional<T>) dao.get(id);
    }

    public <T extends Entity> Stream<T> get(Class<T> clazz) throws DataAccessException {
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return dao.get();
    }

    public <T extends Entity> boolean delete(Class<T> clazz, int id) throws DataAccessException {
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return dao.delete(id);
    }

    public <T extends Entity> boolean delete(T object) throws DataAccessException {
        return delete(object.getClass(), object.getId());
    }

    @SuppressWarnings("unchecked")
    public <T extends Entity> void insert(T object) throws DataAccessException {
        Class<T> clazz = (Class<T>) object.getClass();
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        dao.insert(object);
    }

    public <T extends Entity> void insert(Iterable<T> objs) throws DataAccessException {
        Iterator<T> iterator = objs.iterator();

        if(!iterator.hasNext()) return;

        T firstElement = iterator.next();
        if(firstElement == null) throw new IllegalArgumentException("No debe pasar objeto nulos");

        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>) firstElement.getClass();
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        Iterable<T> reObjs = () -> new Iterator<T>() {
            private boolean feConsumed = false;

            @Override
            public boolean hasNext() {
                return !feConsumed || iterator.hasNext();
            }

            @Override
            public T next() {
                if(!feConsumed) {
                    feConsumed = true;
                    return firstElement;
                }
                else return iterator.next();
            }
        };

        dao.insert(reObjs);
    }

    @SuppressWarnings("unchecked")
    public <T extends Entity> void insert(T[] objs) throws DataAccessException {
        if(objs.length == 0) return;

        Class<T> clazz = (Class<T>) objs[0].getClass();
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        dao.insert(objs);
    }

    @SuppressWarnings("unchecked")
    public <T extends Entity> boolean update(T object) throws DataAccessException {
        Class<T> clazz = (Class<T>) object.getClass();
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return ((Crud<T>) dao).update(object);
    }

    public <T extends Entity> boolean update(Class<T> clazz, int oldId, int newId) throws DataAccessException {
        Crud<T> dao = getDaoClass(clazz);
        if(dao == null) throw new DataAccessRuntimeException(String.format("La clase '%s' no se registro como clase DAO", clazz.getName()));

        return dao.update(oldId, newId);
    }

    public <T extends Entity> boolean update(T object, int newId) throws DataAccessException {
        return update(object.getClass(), object.getId(), newId);
    }
}
