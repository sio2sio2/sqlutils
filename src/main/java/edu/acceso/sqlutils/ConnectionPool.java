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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Pool de conexiones para manejar múltiples conexiones a una base de datos.
 * Utiliza {@link HikariDataSource} y un patrón Multiton para garantizar
 * que solo haya una instancia por URL de conexión, usuario y
 * contraseña.
 */
public class ConnectionPool implements AutoCloseable {

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
     * @throws IllegalStateException Si la instancia ya existe.
     */
    public static ConnectionPool create(String key, String dbUrl, String user, String password) {
        Objects.requireNonNull(key, "La clave no puede ser nula");

        if(instances.containsKey(key)) throw new IllegalStateException("Ya hay una instancia asociada a la clave");

	    ConnectionPool instance = new ConnectionPool(key, dbUrl, user, password);
        ConnectionPool previa = instances.putIfAbsent(key, instance);

        // Otro hilo generó una instancia.
        if(previa != null) {
            instance.close();
            throw new IllegalStateException("Ya hay una instancia asociada a la clave");
        }

        return instance;
    }

    /**
     * Genera una instancia {@link ConnectionPool} cuando no son necesarios usuario ni contraseña.
     * @param url La URL de conexión.
     * @return La instancia solicitada.
     */
    public static ConnectionPool create(String key, String dbUrl) {
        return ConnectionPool.create(key, dbUrl, null, null);
    }

    /**
     * Obtiene la instancia asociada a una clave.
     * @return La instancia solicitada.
     * @throws IllegalArgumentException Si no existe la instancia para la clave suministrada.
     */
    public static ConnectionPool get(String key) {
        ConnectionPool instance = instances.get(key);
        if(instance == null) throw new IllegalStateException("No existe una instancia para la clave %s".formatted(key));

        if(instance.isOpen()) return instance;
        else {
            instances.remove(key, instance);
            throw new IllegalStateException("La instancia solicitada no existe.");
        }
    }

    public boolean isOpen() {
        return !closed.get() && !ds.isClosed();
    }

    public DataSource getDataSource() {
        return ds;
    }

    @Override
    public void close() {
        if(closed.compareAndSet(false, true)) {
            instances.remove(key, this);
            ds.close();
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
                    
                    // Filtrar tablas del sistema según el SGBD
                    if (!isGenericSystemTable(tableName)) {
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
}
