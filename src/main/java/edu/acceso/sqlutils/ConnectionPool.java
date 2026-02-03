package edu.acceso.sqlutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.Objects;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import edu.acceso.sqlutils.tx.TransactionManager;

/**
 * Pool de conexiones para manejar múltiples conexiones a una base de datos.
 * Utiliza {@link HikariDataSource} y un patrón Multiton para garantizar
 * que solo haya una instancia por URL de conexión, usuario y
 * contraseña.
 */
public class ConnectionPool implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    /** Mapa con las instancias creadas **/
    private static final Map<String, ConnectionPool> instances = new ConcurrentHashMap<>();
    /** Número máximo de conexiones en el pool */
    public static volatile short maxConnections = 10;
    /** Número mínimo de conexiones en el pool */
    public static volatile short minConnections = 1;

    private final String key;
    private final HikariDataSource ds;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ConnectionPool(String key, String dbUrl, String user, String password) {
        HikariConfig hconfig = new HikariConfig();
        hconfig.setJdbcUrl(dbUrl);
        hconfig.setUsername(user);
        hconfig.setPassword(password);
        // Mínimo y máximo de conexiones.
        hconfig.setMaximumPoolSize(maxConnections);
        hconfig.setMinimumIdle(minConnections);

        ds = new HikariDataSource(hconfig);
        this.key = key;
    }

    /**
     * Crea una instancia a partir de los datos de conexión.
     * @param key Clave que identifica la conexión.
     * @param dbUrl URL de conexión a la base de datos.
     * @param user Usuario de conexión
     * @param password Contraseña de conexión
     * @return La instancia solicitada.
     * @throws IllegalStateException Si la instancia ya existe.
     */
    public static ConnectionPool create(String key, String dbUrl, String user, String password) {
        Objects.requireNonNull(key, "La clave no puede ser nula");

        if(instances.containsKey(key)) throw new IllegalStateException("Ya hay una instancia asociada a la clave");

	    ConnectionPool instance = new ConnectionPool(key, dbUrl, user, password);
        logger.debug("Creado nuevo pool de conexiones para la clave {}", key);
        ConnectionPool previa = instances.putIfAbsent(key, instance);

        if(previa != null) {
            logger.debug("Otro hilo creó una instancia para la clave {}: cerrando la recién creada", key);
            instance.close();
            throw new IllegalStateException("Ya hay una instancia asociada a la clave");
        }

        return instance;
    }

    /**
     * Genera una instancia {@link ConnectionPool} cuando no son necesarios usuario ni contraseña.
     * @param key Clave que identifica la conexión.
     * @param dbUrl La URL de conexión.
     * @return La instancia solicitada.
     */
    public static ConnectionPool create(String key, String dbUrl) {
        return ConnectionPool.create(key, dbUrl, null, null);
    }

    /**
     * Obtiene la instancia asociada a una clave.
     * @param key Clave que identifica la conexión.
     * @return La instancia solicitada.
     * @throws IllegalArgumentException Si no existe la instancia para la clave suministrada.
     */
    public static ConnectionPool get(String key) {
        ConnectionPool instance = instances.get(key);
        if(instance == null) throw new IllegalStateException("No existe una instancia para la clave %s".formatted(key));

        if(instance.isOpen()) {
            logger.debug("Hallado pool abierto de conexiones para la clave {}", key);
            return instance;
        }
        else {
            logger.debug("Hallado pool de conexiones para la clave {}, pero no está abierto", key);
            instances.remove(key, instance);
            throw new IllegalStateException("La instancia solicitada no existe.");
        }
    }

    /** Comprueba si el pool de conexiones está abierto
     * @return {@code true} si el pool está abierto, {@code false} en caso contrario
     */
    public boolean isOpen() {
        return !closed.get() && !ds.isClosed();
    }

    /** Obtiene el DataSource asociado al pool de conexiones
     * @return El DataSource asociado
     */
    public DataSource getDataSource() {
        try {
            TransactionManager.get(key);
            logger.warn("Hay un gestor de transacciones asociado a este ConnectionPool. A menos de que esté seguro de lo que hace, debería obtener las conexiones a través de él.");
        } catch(IllegalStateException e) {}

        return ds;
    }

    /** Crea un gestor de transacciones {@link TransactionManager} asociado a este pool de conexiones */
    public void setTransactionManager() {
        setTransactionManager(TransactionManager.class);
    }

    /**
     * Crea un gestor de transacciones asociado a este pool de conexiones.
     * @param tmClass La clase derivada de {@link TransactionManager} que se va a instanciar.
     */
    public void setTransactionManager(Class<? extends TransactionManager> tmClass) {
        if(!isOpen()) throw new IllegalStateException("El ConnectionPool está cerrado");
        try {
            TransactionManager.create(key, ds, tmClass);
            logger.debug("Creado un gestor de transacciones {} asociado a la clave {} de este ConnectionPool", tmClass.getSimpleName(), key);
        } catch(IllegalStateException e) {
            logger.warn("Ya existe un gestor de transacciones asociado a la clave {}. Quizás lo creó manualmente.", key);
        }
    }

    /**
     * Obtiene el gestor de transacciones asociado a este pool de conexiones.
     * @return El gestor de transacciones asociado.
     */
    public TransactionManager getTransactionManager() {
        if(!isOpen()) throw new IllegalStateException("El ConnectionPool está cerrado");
        try {
            return TransactionManager.get(key);
        } catch(IllegalStateException e) {
            throw new IllegalStateException("No hay un gestor de transacciones asociado a este ConnectionPool. Debe crearlo primero con setTransactionManager().");
        }
    }

    @Override
    public void close() {
        if(closed.compareAndSet(false, true)) {
            instances.remove(key, this);
            ds.close();
            logger.debug("Pool de conexiones cerrado para la clave {}", key);
        }
    }

    /**
     * Verifica si la base de datos ya ha sido inicializada.
     * @return true si la base de datos tiene al menos una tabla de usuario, false en caso contrario.
     * @throws SQLException Si ocurre un error al acceder a los metadatos de la base de datos.
     */
    public boolean isDatabaseInitialized() throws SQLException {
        try(Connection conn = ds.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();

            // 1. Intentamos obtener catálogo y esquema actuales
            String catalog = conn.getCatalog();
            String schema = null;
            
            try {// getSchema() no existe en versiones muy antiguas de JDBC
                schema = conn.getSchema(); 
            } catch (AbstractMethodError | SQLException e) {
                // Evitamos fallos con versiones obsoletas de JDBC
            }

            try (ResultSet tables = metaData.getTables(catalog, schema, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    logger.trace("Hallada tabla en la base de datos: {}. Comprobando si es tabla del sistema...", tableName);
                    
                    // Filtrar tablas del sistema según el SGBD
                    if (!isGenericSystemTable(tableName)) {
                        logger.debug("{} es una tabla de usuario. Se presupone que la base de datos está completamente inicializada.", tableName);
                        return true; // Encontramos al menos una tabla de usuario
                    }
                }
            }
            return false;
        }
    }

    /**
     * Verifica si el nombre de la tabla corresponde a una tabla del sistema genérica.
     * @param tableName El nombre de la tabla.
     * @return true si es una tabla del sistema, false en caso contrario.
     */
    private static boolean isGenericSystemTable(String tableName) {
        tableName = tableName.toLowerCase();
        
        // Lista de tablas del sistema comunes en varios SGBD
        return  tableName.startsWith("sys") ||               // Oracle, MSSQL, Derby, DB2, H2
                tableName.contains("information_schema") ||  // MariaDB, MSSQL, MySQL, SQL Server, H2
                tableName.startsWith("databasechangelog") || // Liquibase
                tableName.startsWith("flyway_") ||           // Flyway
                tableName.startsWith("pg_") ||               // PostgreSQL
                tableName.startsWith("msrepl") ||            // MSSQL Replication
                tableName.startsWith("sqlite_");             // SQLite
    }

    /**
     * Obtiene la clave que identifica este pool de conexiones.
     * @return La clave solicitada.
     */
    public String getKey() {
        return key;
    }
}
