package edu.acceso.sqlutils.dao;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Permite escoger la conexión en tiempo de ejecución.
 */
public class BackendFactory {

    /**
     * Clases para conectar con distintos SGBD.
     */
    private final Map<String, Class<? extends DaoConnection>> connections;

    /**
     * Constructor de la clase.
     */
    public BackendFactory() {
        connections = new HashMap<>();
    }

    /**
     * Registra una clase de conexión a un SGBD.
     * @param name Identifica al SGBD al que se da soporte.
     * @param connector La clase que permite la conexión.
     * @return El propio objeto.
     */
    public BackendFactory register(String name, Class<? extends DaoConnection> connector) {
        connections.put(name, connector);
        return this;
    }

    /**
     * Obtiene la clase de conexión a un SGBD a partir del nombre asociado a él.
     * @param name El nombre que identifica al SGBD.
     * @return La clase que habilita la conexión.
     */
    private Class<? extends DaoConnection> getConnClass(String name) {
        return connections.keySet().stream()
            .filter(k -> k.toLowerCase().equals(name.toLowerCase()))
            .map(k -> connections.get(k))
            .findFirst().orElseThrow(() -> new IllegalArgumentException(String.format("'%s': formato desconocido", name)));
    }

    /**
     * Selecciona el constructor adecuado en función de cómo se quiere inicializar la base de datos.
     * @param clazz La clase de conexión para la que se quiere seleccionar el constructor.
     * @param argument El argumento que permite inicializar la base de datos.
     * @return El constructor adecuado.
     * @throws NoSuchMethodException Cuando no existe constructor para ese argumento de inicialización.
     */
    private static Constructor<?> selectConstructor(Class<?> clazz, Object argument) throws NoSuchMethodException {
        if(argument == null) return clazz.getDeclaredConstructor(Map.class, Class[].class);

        Class<?> argumentClass = argument.getClass();

        try {
            return clazz.getDeclaredConstructor(Map.class, argumentClass, Class[].class);
        } catch(NoSuchMethodException err) {
            for(var ctor: clazz.getConstructors()) {
                Class<?>[] argumentTypes = ctor.getParameterTypes();
                if(argumentTypes.length != 3) continue;
                if(argumentTypes[1].isAssignableFrom(argumentClass)) return ctor;
            }
            throw new NoSuchMethodException(String.format("La clase '%s' no implementa un constructor que admita un objeto %s", clazz, argumentClass.getSimpleName()));
        }
    }

    /**
     * Crea un objeto de conexión sin método para inicializar la base de datos.
     * @param name El nombre que identifica al SGBD.
     * @param opciones Las opciones para establecer la conexión.
     * @param daoClasses Las clases DAO que se usarán para hacer persistentes entidades en la base de datos
     * @return El objeto de conexión adecuado.
     */
    public DaoConnection createConnection(String name, Map<String, Object> opciones, Class<?> ... daoClasses) {
        return createConnection(name, opciones, null, daoClasses);
    }

    /**
     * Crea un objeto de conexión que intenta inicializar la base de datos.
     * @param name El nombre que identifica al SGBD.
     * @param opciones Las opciones para establecer la conexión.
     * @param initializer El modo en que se inicializa la base de datos. Véase {@link Dao} para ver qué inicializadores son válidos.
     * @param daoClasses Las clases DAO que se usarán para hacer persistentes entidades en la base de datos
     * @return El objeto de conexión adecuado.
     */
    public DaoConnection createConnection(String name, Map<String, Object> opciones, Object initializer, Class<?> ... daoClasses) {
        Objects.requireNonNull(name, "El nombre no puede ser nulo");

        Class<? extends DaoConnection> connector = getConnClass(name);

        if(connector == null) throw new UnsupportedOperationException(String.format("'%s': formato no soportado", name));

        try {
            Constructor<?> ctor = selectConstructor(connector, initializer);
            return (DaoConnection) (initializer == null?ctor.newInstance(opciones, daoClasses):ctor.newInstance(opciones, initializer, daoClasses));
        } catch (ReflectiveOperationException err) {
            throw new RuntimeException(err);
        }

    }
}
