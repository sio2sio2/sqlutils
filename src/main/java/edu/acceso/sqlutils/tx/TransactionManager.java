package edu.acceso.sqlutils.tx;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.ConnectionWrapper;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Gestor multihilo para manejar transacciones de base de datos.
 * <p>Cuando se ha abierto una transacción, proporciona una conexión por hilo.
 * Permite anidar transacciones y soporta multiples bases de datos mediante una clave identificativa.
 * Implementa el patrón Multiton (varias instancias singleton diferenciadas por una clave).
 * <p>Para crear una instancia, use el método estático {@link #create(String, DataSource)}:
 * <pre>
 *    TransactionManager tm = TransactionManager.create("gestor1", dataSource);
 * </pre>
 * <p>Una vez creada la instancia, es recomendable usar sus métodos {@link #transaction(TransactionableR)}
 * o {@link #transaction(Transactionable)} para ejecutar operaciones dentro de una transacción:
 * <pre>
 *    tm.transaction(conn -&gt; {
 *       // operaciones con la conexión dentro de una transacción
 *       // La conexión está protegida contra el cierre accidental
 *    });
 * </pre>
 * <p>Si se decide manejar manualmente la creación y cierrre de transacciones, puede consultarse el código
 * del método {@link #transaction(TransactionableR)} para ver cómo hacerlo correctamente.
 */
public class TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    /** Conexión por hilo para manejar transacciones anidadas */
    private final ThreadLocal<Connection> connectionHolder;
    /** Contador de niveles de transacción por hilo */
    private final ThreadLocal<Integer> counter;
    /** Auto-commit original por hilo */
    private final ThreadLocal<Boolean> originalAutoCommit;

    /** Instancias de gestores de transacciones accesibles por clave */
    private static final Map<String, TransactionManager> instances = new ConcurrentHashMap<>();

    /**
     * El DataSource asociado a cada gestor de transacciones.
     */
    private final DataSource ds;
    /**
     * La clave asociada a la instancia.
     */
    private final String key;

    /**
     * Interfaz funcional para operaciones de transacción que devuelven un valor.
     * @param <T> Tipo del valor devuelto.
     */
    @FunctionalInterface
    public static interface TransactionableR<T> {
        T run(Connection conn) throws DataAccessException;
    }

    /**
     * Interfaz funcional para operaciones de transacción sin valor devuelto.
     */
    @FunctionalInterface
    public static interface Transactionable {
        void run(Connection conn) throws DataAccessException;
    }

    /** Constructor privado para implementar el patrón Multiton */
    private TransactionManager(String key, DataSource ds) {
        this.key = key;
        this.ds = ds;
        connectionHolder = new ThreadLocal<>();
        counter = ThreadLocal.withInitial(() -> 0);
        originalAutoCommit = new ThreadLocal<>();
    }

    /**
     * Crea un gestor de transacciones asociado a una clave y un DataSource.
     * @param key Clave identificativa del gestor.
     * @param ds DataSource asociado al gestor.
     * @return El gestor de transacciones creado.
     * @throws IllegalStateException Si el gestor ya ha sido inicializado.
     */
    public static TransactionManager create(String key, DataSource ds) {
        TransactionManager tm = new TransactionManager(key, ds);
        if(instances.putIfAbsent(key, tm) != null) throw new IllegalStateException("TransactionManager ya ha sido inicializado.");

        return tm;
    }

    /**
     * Obtiene el gestor de transacciones asociado a una clave.
     * @param key Clave identificativa.
     * @return El gestor de transacciones asociado a la clave.
     */
    public static TransactionManager get(String key) {
        TransactionManager tm = instances.get(key);
        if(tm == null) throw new IllegalStateException("El gestor de transacciones '%s' no ha sido inicializado. Llame a create() primero.".formatted(key));
        return tm;
    }

    /**
     * Inicia una transacción creando una conexión para ello. Si ya hay una
     * transacción activa, simplemente incrementa el contador.
     * @throws SQLException Si ocurre un error al iniciar la transacción.
     */
    public void begin() throws SQLException {
        if(counter.get() > 0) {
            counter.set(counter.get() + 1);
            return;
        }

        Connection conn = ds.getConnection();
        originalAutoCommit.set(conn.getAutoCommit());
        conn.setAutoCommit(false);
        connectionHolder.set(conn);
        counter.set(counter.get() + 1);
    }

    /**
     * Obtiene la conexión asociada a la transacción actual para resultados
     * que no son flujos (listas, etc.). Se caracteriza porque las sentencias
     * creadas a partir de esta conexión se pueden cerar manualmente antes del cierre
     * de la conexión.
     * @return La conexión asociada a la transacción actual.
     */
    public Connection getConnectionForList() {
        if(!isActive()) throw new IllegalStateException("Debe abrir una transacción para disponer de una conexión");
        return ConnectionWrapper.createProxy(connectionHolder.get(), false);
    }

    /**
     * Obtiene la conexión asociada a la transacción actual.
     * @return La conexión asociada a la transacción actual.
     */
    public Connection getConnection() {
        if(!isActive()) throw new IllegalStateException("Debe abrir una transacción para disponer de una conexión");
        return ConnectionWrapper.createProxy(connectionHolder.get(), true);
    }

    /**
     * Obtiene la conexión asociada a la transacción del gestor identificado por la clave.
     * @param key Clave identificativa del gestor.
     * @return La conexión asociada a la transacción referida por la clave.
     */
    public static Connection getConnection(String key) {
        return get(key).getConnection();
    }

    /**
     * Obtiene la conexión para listas asociada a la transacción del gestor identificado por la clave.
     * @param key Clave identificativa del gestor.
     * @return La conexión para listas asociada a la transacción referida por la clave.
     */
    public static Connection getConnectionForList(String key) {
        return get(key).getConnectionForList();
    }

    /**
     * Confirma la transacción actual, a menos que la transacción esté anidada,
     * en cuyo caso simplemente decrementa el contador.
     * @throws SQLException Si ocurre un error al confirmar la transacción.
     */
    public void commit() throws SQLException {
        switch(counter.get()) {
            case 0:
                throw new SQLException("No hay transacción activa. Debe comenzarla con begin().");
            case 1:
                Connection conn = connectionHolder.get();

                if (conn != null) {
                    try {
                        conn.commit();
                        conn.setAutoCommit(originalAutoCommit.get());
                        conn.close();
                    } finally {
                        connectionHolder.remove();
                        originalAutoCommit.remove();
                        counter.remove();
                    }
                }
                break;
            default:
                counter.set(counter.get() - 1);
        }
    }

    public void rollbackandThrow(Throwable e) throws DataAccessException {
        Connection conn = connectionHolder.get();

        try {
            if(conn != null && !conn.isClosed()) {
                conn.rollback();
                conn.setAutoCommit(originalAutoCommit.get());
                conn.close();
            }
        } catch (SQLException ex) {
            logger.error("Error al deshacer la transacción", ex);
            if(e != null) e.addSuppressed(ex);
        } finally {
            connectionHolder.remove();
            originalAutoCommit.remove();
            counter.remove();
        }
        if(e instanceof RuntimeException re) throw re;
        if(e instanceof DataAccessException re) throw re;
        if(e instanceof Error re) throw re;
        else throw new DataAccessException("Error en la transacción", e);
    }

    /**
     * Ejecuta operaciones dentro de una transacción.
     * @param <T> Tipo del valor devuelto.
     * @param operations Operaciones a ejecutar dentro de la transacción.
     * @return El valor devuelto por las operaciones.
     * @throws DataAccessException Si ocurre un error durante las operaciones
     */
    public <T> T transaction(TransactionableR<T> operations) throws DataAccessException {
        T value = null;

        try {
            begin();
            value = operations.run(this.getConnection());
            commit();
        } catch (Throwable e) {
            rollbackandThrow(e);
        }

        return value;
    }

    /**
     * Ejecuta operaciones dentro de una transacción.
     * @param operations Operaciones a ejecutar dentro de la transacción.
     * @throws DataAccessException Si ocurre un error durante las operaciones
     */
    public void transaction(Transactionable operations) throws DataAccessException {
        transaction(conn -> {
            operations.run(conn);
            return null;
        });
    }

    /**
     * Indica si en el hilo actual la transacción está abierta.
     * @return `true`, si la transacción está abierta.
     */
    public boolean isActive() {
        return counter.get() > 0;
    }

    /**
     * Devuelve la clave asociada a la instancia.
     * @return La clave asociada a la instancia.
     */
    public String getKey() {
        return key;
    }

    /**
     * Obtiene los nombres de los gestores de transacciones disponibles.
     * @return Los nombres de los gestores de transacciones.
     */
    public static String[] getKeys() {
        return instances.keySet().toArray(new String[0]);
    }
}
