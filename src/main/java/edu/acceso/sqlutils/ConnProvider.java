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
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Proveedor de conexiones SQL que permite, al invocarse su método {@link #getConnection()},
 * obtener conexiones {@link Connection} a la base de datos.
 * 
 * <p>
 * Si se construye suministrando un {@link DataSource}, cada vez que se invoca el método
 * {@link #getConnection()} se genera una nueva conexión a través del pool. En cambio,
 * si se construye con una conexión existente, se reutiliza esa conexión, pero esta no
 * se cierra al invocarse el método {@link Connection#close()}.
 * <p>
 * La clase está pensada para ser utilizada en las clases DAO, e impedir que el código
 * incluido en dichas clases cierre la conexión accidentamente cuando los objetos DAO
 * se construyen usando una conexión en vez de un {@link DataSource}. En consecuencia,
 * es irrelevante si se generó el objeto {@link ConnProvider} con un {@link DataSource}
 * o con una conexión existente, a efectos de uso. siempre podremos hacer:
 * <pre>
 *     // cp es un objeto ConnProvider creado con un DataSource o una conexión existente
 *     try(Connection conn = cp.getConnection()) {
 *         // Aquí se usa la conexión conn
 *     } catch (SQLException e) {
 *         // Manejo de excepciones
 *     }
 * </pre>
 */
public class ConnProvider implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ConnProvider.class);

    /** DataSource utilizado para obtener conexiones. */
    private final DataSource ds;
    /** Conexión utilizada si se construye el proveedor con una conexión existente. */
    private final Connection conn;

    /**
     *  Wrapper para conexiones que evita que se cierren al invocar close().
     *  Utiliza el patrón Proxy de Java para interceptar llamadas a métodos de la interfaz Connection.
     */
    private static class ConnectionWrapper implements InvocationHandler {

        /** Conexión original que se envuelve. */
        private final Connection conn;
        private final Set<Statement> statements = Collections.synchronizedSet(new HashSet<>());

        /**
         * Constructor privado que crea un wrapper para una conexión.
         * @param conn La conexión original a envolver.
         */
        private ConnectionWrapper(Connection conn) {
            this.conn = conn;
        }

        /**
         * Crea un proxy de la conexión original que evita que se cierre al invocar close().
         * @param conn La conexión original a envolver.
         * @return Un objeto Connection que actúa como proxy de la conexión original.
         */
        public static Connection createProxy(Connection conn) {
            return (Connection) Proxy.newProxyInstance(
                conn.getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                new ConnectionWrapper(conn)
            );
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
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
                    // Evitamos cerrar la conexión al invocar close().
                    case "close":
                        statements.forEach(statement -> {
                            try {
                                statement.close();
                            } catch (SQLException e) {
                                logger.error("Error closing statement: " + statement, e);
                            }
                        });
                        return null;
                    // Cualquier otro método de la interfaz Connection
                    // se delega al objeto Connection original.
                    default:
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
    }

    /**
     * Constructor que inicializa el proveedor de conexiones con un {@link DataSource}.
     * @param ds Fuente de datos para obtener conexiones.
     */
    public ConnProvider(DataSource ds) {
        this.ds = ds;
        conn = null;
    }

    /**
     * Constructor que inicializa el proveedor de conexiones con una conexión existente.
     * @param conn Conexión existente a utilizar.
     */
    public ConnProvider(Connection conn) {
        ds = null;
        this.conn = conn;
    }

    /**
     * Obtiene una conexión a la base de datos.
     * Si se construyó con un {@link DataSource}, devuelve una nueva conexión del pool.
     * Si se construyó con una conexión existente, devuelve esa conexión envuelta en un proxy.
     * @return Una conexión a la base de datos.
     * @throws DataAccessException Si ocurre un error al obtener la conexión.
     */
    public Connection getConnection() throws DataAccessException {
        if(isDataSource()) {
            try {
                return ds.getConnection();
            } catch (SQLException e) {
                throw new DataAccessException(e);
            }
        }
        else return ConnectionWrapper.createProxy(conn);
    }

    /**
     * Comprueba si el proveedor de conexiones se construyó con un {@link DataSource}.
     * @return true si se construyó con un DataSource, false si se construyó con una conexión.
     */
    public boolean isDataSource() {
        return ds != null;
    }

    /**
     * Cierra la conexión o el DataSource, dependiendo de cómo se construyó el proveedor.
     * Si se construyó con un {@link DataSource}, cierra el DataSource, siempre que sea posible.
     * Si se construyó con una {@link Connection} existente, cierra esa conexión.
     * @throws DataAccessException Si ocurre un error al cerrar la conexión o el DataSource.
     */
    @Override
    public void close() throws DataAccessException {
        if(isDataSource()) {
            try {
                if(ds instanceof AutoCloseable) ((AutoCloseable) ds).close();
            } catch (Exception e) {
                throw new DataAccessException(e);
            }
        } else {
            try {
                conn.close();
            } catch (SQLException e) {
                throw new DataAccessException(e);
            }
        }
    }
}