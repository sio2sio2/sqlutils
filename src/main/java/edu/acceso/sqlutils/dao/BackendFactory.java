package edu.acceso.sqlutils.dao;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

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

    public DaoConnection createConnection(String name, Map<String, Object> opciones, Class<?> ... daoClasses) {
        Class<? extends DaoConnection> connector = null;

        try {
            connector = getConnClass(name);
        }
        catch(IllegalArgumentException err) {
            throw err;
        }

        if(connector == null) throw new UnsupportedOperationException(String.format("'%s': formato no soportado", name));

        try {
            return connector.getDeclaredConstructor(Map.class, Class[].class).newInstance(opciones, daoClasses);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException err) {
            throw new RuntimeException(err);
        }
    }
}
