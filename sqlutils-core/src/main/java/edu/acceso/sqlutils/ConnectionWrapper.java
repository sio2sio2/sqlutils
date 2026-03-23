package edu.acceso.sqlutils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Wrapper para conexiones que evita que se cierren al invocar close().
 *  Utiliza el patrón Proxy de Java para interceptar llamadas a métodos de la interfaz Connection.
 */
public class ConnectionWrapper implements InvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionWrapper.class);

    /** Conexión original que se envuelve. */
    private final Connection conn;
    private final boolean forStream;

    /**
     * Constructor privado que crea un wrapper para una conexión.
     * @param conn La conexión original a envolver.
     * @param forStream Indica si la conexión se usará para Streams en cuyo caso no se cerrarán las sentencias
     * abiertas (aunque explícitamente se invoque su método close()) hasta que no se cierre la propia conexión.
     */
    private ConnectionWrapper(Connection conn, boolean forStream) {
        this.conn = conn;
        this.forStream = forStream;
    }

    /**
     * Crea un proxy de la conexión original que evita que se cierre al invocar close().
     * @param conn La conexión original a envolver.
     * @param forStream Indica si la conexión se usará para obtener resultados que sean Streams en cuyo caso no se cerrarán las sentencias
     * abiertas (aunque explícitamente se invoque su método close()) hasta que no se cierre la propia conexión.
     * @return Un objeto Connection que actúa como proxy de la conexión original.
     */
    public static Connection createProxy(Connection conn, boolean forStream) {
        Objects.requireNonNull(conn, "No puede envolverse una conexión nula");

        return (Connection) Proxy.newProxyInstance(
            conn.getClass().getClassLoader(),
            new Class<?>[]{Connection.class},
            new ConnectionWrapper(conn, forStream)
        );
    }

    /**
     * Crea un proxy de la conexión original que evita que se cierre al invocar close().
     * Las conexiones creadas con este método están pensadas para usarse con Streams.
     * @param conn La conexión original a envolver.
     * @return Un objeto Connection que actúa como proxy de la conexión original.
     */
    public static Connection createProxy(Connection conn) {
        return createProxy(conn, true);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            switch(method.getName()) {
                // El proxy se considera igual a sí mismo.
                case "equals":
                    return proxy == args[0];
                case "hashCode":
                    return System.identityHashCode(proxy);
                // Evitamos cerrar la conexión al invocar close().
                case "close":
                    logger.trace("Capturada llamada a close(): la conexión no se cierra.");
                    return null;
                default:
                    Object result = method.invoke(conn, args);
                    if(isForStream()) {
                        // Cualquier otro método de la interfaz Connection
                        // se delega al objeto Connection original.
                        switch(method.getName()) {
                            // Debemos interceptar los métodos que crean sentencias,
                            // para que tampoco se puedan cerrar manualmente.
                            case "createStatement":
                                return StatementWrapper.createProxy((Statement) result, Statement.class);
                            case "prepareStatement":
                                return StatementWrapper.createProxy((PreparedStatement) result, PreparedStatement.class);
                            case "prepareCall":
                                return StatementWrapper.createProxy((CallableStatement) result, CallableStatement.class);
                        }
                    }
                    return result;
            }
        }
        catch(InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private static class StatementWrapper implements InvocationHandler {
        private final Statement stmt;

        private StatementWrapper(Statement stmt) {
            this.stmt = stmt;
        }

        /**
         * Crea un proxy para una sentencia que evita que se cierre al invocar close().
         * @param stmt La sentencia original a envolver.
         * @param type El tipo de la sentencia (Statement, PreparedStatement, CallableStatement).
         * @return Un objeto Statement que actúa como proxy de la sentencia original.
         */
        public static <T extends Statement> T createProxy(T stmt, Class<T> type) {
            return type.cast(Proxy.newProxyInstance(
                stmt.getClass().getClassLoader(),
                getInterfaces(stmt),
                new StatementWrapper(stmt)
            ));
        }

        /**
         * Obtiene las interfaces que implementa la sentencia original.
         * @param stmt La sentencia original.
         * @return Un arreglo de clases que representan las interfaces implementadas por la sentencia.
         */
        private static Class<?>[] getInterfaces(Statement stmt) {
            List<Class<?>> interfaces = new ArrayList<>();
            interfaces.add(Statement.class);
            if (stmt instanceof PreparedStatement) {
                interfaces.add(PreparedStatement.class);
            }
            if (stmt instanceof CallableStatement) {
                interfaces.add(CallableStatement.class);
            }
            return interfaces.toArray(new Class<?>[0]); 
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            switch(method.getName()) {
                // Interceptamos close() para eliminarla del Set de statements.
                case "close":
                    logger.trace("Capturada llamada a close(): la sentencia no se cierra.");
                    return null;
                default:
                    try {
                        return method.invoke(stmt, args);
                    } catch (InvocationTargetException e) {
                        throw e.getTargetException();
                    }
            }
        }
    }

    /**
     * Indica si las conexiones obtenidas con este proveedor están pensadas para usarse con Streams.
     * @return {@code true} si la conexión es para Streams, {@code false} en caso contrario
     */
    public boolean isForStream() {
        return forStream;
    }
}