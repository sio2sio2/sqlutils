package edu.acceso.sqlutils.dao;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class BackendFactory {

    private final Map<String, Class<? extends DaoConnection>> connections;

    public BackendFactory() {
        connections = new HashMap<>();
    }

    public BackendFactory register(String name, Class<? extends DaoConnection> connector) {
        connections.put(name, connector);
        return this;
    }

    private Class<? extends DaoConnection> getConnClass(String name) {
        return connections.keySet().stream()
            .filter(k -> k.toLowerCase().equals(name.toLowerCase()))
            .map(k -> connections.get(k))
            .findFirst().orElseThrow(() -> new IllegalArgumentException(String.format("'%s': formato desconocido", name)));
    }

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

    public DaoConnection createConnection(String name, Map<String, Object> opciones, Class<?> ... daoClasses) {
        return createConnection(name, opciones, null, daoClasses);
    }

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
