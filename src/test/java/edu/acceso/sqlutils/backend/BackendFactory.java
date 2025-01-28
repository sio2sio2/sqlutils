package edu.acceso.sqlutils.backend;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

import edu.acceso.sqlutils.backend.sqlite.ConexionSqlite;
import edu.acceso.sqlutils.dao.DaoConnection;

public class BackendFactory {

    static enum Backend {
        SQLITE(ConexionSqlite.class),
        MYSQL(null),
        XML(null);

        private Class<? extends DaoConnection> backend;

        Backend(Class<? extends DaoConnection> backend) {
            setBackend(backend);
        }

        private void setBackend(Class<? extends DaoConnection> backend) {
            this.backend = backend;
        }

        public Class<? extends DaoConnection> get() {
            return backend;
        }

        public static Backend getBackend(String backend) {
            return Arrays.stream(Backend.values())
                    .filter(f -> f.name().toLowerCase().equals(backend.toLowerCase()))
                    .findFirst().orElse(null);
        }

        public boolean noImplementado() {
            return backend == null;
        }
    };

    public static DaoConnection crearConexion(Map<String, Object> opciones) {
        Backend backend = Backend.getBackend((String) opciones.get("base"));
        if(backend == null) throw new IllegalArgumentException(String.format("'%s': formato desconocido", opciones.get("base")));
        if(backend.noImplementado()) throw new UnsupportedOperationException(String.format("'%s': formato no soportado", opciones.get("base")));

        try {
            return backend.get().getDeclaredConstructor(Map.class).newInstance(opciones);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException err) {
            throw new RuntimeException(err);
        }
    }
}