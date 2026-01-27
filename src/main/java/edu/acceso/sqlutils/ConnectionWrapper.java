package edu.acceso.sqlutils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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
    private Set<Statement> statements;

    /**
     * Constructor privado que crea un wrapper para una conexión.
     * @param conn La conexión original a envolver.
     */
    private ConnectionWrapper(Connection conn, boolean forStream) {
        this.conn = conn;
        statements = forStream ? Collections.synchronizedSet(new HashSet<>()) : null;
    }

    /**
     * Crea un proxy de la conexión original que evita que se cierre al invocar close().
     * @param conn La conexión original a envolver.
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
                    if(isForStream()) {
                        // Cerramos todas las sentencias abiertas.
                        synchronized (statements) {
                            for (Statement statement : statements) {
                                try {
                                    statement.close();
                                } catch (SQLException e) {
                                    logger.error("Error closing statement: " + statement, e);
                                }
                            }
                            statements.clear();
                        }
                    }
                    return null;
                default:
                    if(isForStream()) {
                        // Cualquier otro método de la interfaz Connection
                        // se delega al objeto Connection original.
                        switch(method.getName()) {
                            // Debemos interceptar los métodos que crean sentencias, porque queremos
                            // apuntarlas al conjunto statements para cerrarlas más tarde.
                            case "createStatement":
                                Statement stmt = (Statement) method.invoke(conn, args);
                                statements.add(stmt);
                                return StatementWrapper.createProxy(stmt, this, Statement.class);
                            case "prepareStatement":
                                PreparedStatement pstmt = (PreparedStatement) method.invoke(conn, args);
                                statements.add(pstmt);
                                return StatementWrapper.createProxy(pstmt, this, PreparedStatement.class);
                            case "prepareCall":
                                CallableStatement cstmt = (CallableStatement) method.invoke(conn, args);
                                statements.add(cstmt);
                                return StatementWrapper.createProxy(cstmt, this, CallableStatement.class);
                        }
                    }
                    return method.invoke(conn, args);
            }
        }
        catch(InvocationTargetException e) {
            throw e.getTargetException();
        }
    }

    private static class StatementWrapper implements InvocationHandler {
        private final Statement stmt;
        private final ConnectionWrapper parent;

        private StatementWrapper(Statement stmt, ConnectionWrapper parent) {
            this.stmt = stmt;
            this.parent = parent;
        }

        /**
         * Crea un proxy para una sentencia que evita que se cierre al invocar close().
         * @param stmt La sentencia original a envolver.
         * @param type El tipo de la sentencia (Statement, PreparedStatement, CallableStatement).
         * @return Un objeto Statement que actúa como proxy de la sentencia original.
         */
        public static <T extends Statement> T createProxy(T stmt, ConnectionWrapper cw, Class<T> type) {
            return type.cast(Proxy.newProxyInstance(
                stmt.getClass().getClassLoader(),
                getInterfaces(stmt),
                new StatementWrapper(stmt, cw)
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
                    parent.statements.remove(stmt);
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
     * @return 
     */
    public boolean isForStream() {
        return statements != null;
    }
}