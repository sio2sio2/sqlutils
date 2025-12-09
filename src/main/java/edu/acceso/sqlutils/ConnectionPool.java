package edu.acceso.sqlutils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Pool de conexiones para manejar múltiples conexiones a una base de datos.
 * Utiliza HikariCP como proveedor de conexión y el patrón Singleton
 * para garantizar que solo haya una instancia por combinación de URL, usuario y contraseña.
 */
public class ConnectionPool {

    /** Mapa de instancias de ConnectionPool  para implementar el patrón Singleton ampliado */
    private static Map<Integer, HikariDataSource> instances = new HashMap<>();
    /** Número máximo de conexiones en el pool */
    public static short maxConnections = 10;
    /** Número mínimo de conexiones en el pool */
    public static short minConnections = 1;

    /**
     * Crea un {@link HikariDataSource} con la configuración dada.
     * @param url URL de la base de datos.
     * @param user Usuario de conexión
     * @param password Contraseña de conexión
     */
    private static HikariDataSource createHikariDataSource(String url, String user, String password) {
        HikariConfig hconfig = new HikariConfig();
        hconfig.setJdbcUrl(url);
        hconfig.setUsername(user);
        hconfig.setPassword(password);
        // Mínimo y máximo de conexiones.
        hconfig.setMaximumPoolSize(maxConnections);
        hconfig.setMinimumIdle(minConnections);

        return new HikariDataSource(hconfig);
    }

    /**
     * Genera y almacena una fuente de datos HikariCP con la configuración dada.
     * Si los parámetros de configuración coinciden, genera una excepción
     * @param url URL de la base de datos.
     * @param user Usuario de conexión
     * @param password Contraseña de conexión
     * @return La fuente de datos HikariCP.
     * @throws IllegalArgumentException Si ya existe un pool con la configuración proporcionada.
     */
    public static HikariDataSource getInstance(String url, String user, String password) {
        int hashCode = Objects.hash(url, user, password);
        HikariDataSource instance = instances.get(hashCode);
        if(instance == null || instance.isClosed()) {
            instance = createHikariDataSource(url, user, password);
            instances.put(hashCode, instance);
            return instance;
        }
        else throw new IllegalArgumentException("Ya existe un pool con la configuración proporcionada");
    }

    /**
     * Devuelve un {@link HikariDataSource} cuando no son necesarios usuario ni contraseña.
     * @param url La URL de conexión.
     * @return La fuente de datos HikariCP.
     */
    public static HikariDataSource getInstance(String url) {
        return ConnectionPool.getInstance(url, null, null);
    }

    /**
     * Devuelve un pool de conexiones cuando sólo hay un candidato posible.
     * Como efecto secundario, elimina los pools cuyo DataSource esté cerrado.
     * @return La fuente de datos HikariCP.
     * @throws IllegalArgumentException Si no hay ningún pool activo o si hay varios candidatos.
     */
    public static HikariDataSource getInstance() {
        ConnectionPool.clear();
        switch(instances.size()) {
            case 1:
                HikariDataSource instance = instances.values().iterator().next();
                if(!instance.isClosed()) return instance;
                else instances.clear();
            case 0:
                throw new IllegalArgumentException("No hay definido ningún pool activo");
            default:
                throw new IllegalArgumentException("Ambiguo: hay definidos varios pools");
        }
    }

    /**
     * Elimina los DataSources cerrados del mapa de instancias.
     */
    public static void clear() {
        instances = instances.entrySet()
            .stream().filter(e -> !e.getValue().isClosed())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Cierra todos los pools de conexiones y elimina todas las instancias.
     */
    public static void reset() {
        instances.values().forEach(HikariDataSource::close);
        instances.clear();
    }

    /**
     * Verifica si la base de datos ya ha sido inicializada.
     * @param conn Una conexión a la base de datos.
     * @return true si la base de datos tiene al menos una tabla de usuario, false en caso contrario.
     * @throws SQLException Si ocurre un error al acceder a los metadatos de la base de datos.
     */
    public static boolean isDatabaseInitialized(Connection conn) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        String dbName = metaData.getDatabaseProductName().toLowerCase();
        
        try (ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"})) {
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                
                // Filtrar tablas del sistema según el SGBD
                if (!isSystemTable(dbName, tableName)) {
                    return true; // Encontramos al menos una tabla de usuario
                }
            }
        }
        return false;
    }

    /**
     * Verifica si la base de datos ya ha sido inicializada.
     * @param dataSource La fuente de datos HikariCP.
     * @return true si la base de datos tiene al menos una tabla de usuario, false en caso contrario.
     * @throws SQLException Si ocurre un error al acceder a los metadatos de la base de datos.
     */
    public static boolean isDatabaseInitialized(DataSource dataSource) throws SQLException {
        try(Connection conn = dataSource.getConnection()) {
            return isDatabaseInitialized(conn);
        }
    }

    private static boolean isSystemTable(String dbProduct, String tableName) {
        tableName = tableName.toLowerCase();
        
        if (dbProduct.contains("hsql") || dbProduct.contains("h2")) {
            return tableName.startsWith("information_schema.") || 
                tableName.startsWith("system_");
        } else if (dbProduct.contains("derby")) {
            return tableName.startsWith("sys");
        }
        return false;
    }
}