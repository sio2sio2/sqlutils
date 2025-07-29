package edu.acceso.sqlutils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Pool de conexiones para manejar múltiples conexiones a una base de datos.
 * Utiliza HikariCP como proveedor de conexión y el patrón Singleton
 * para garantizar que solo haya una instancia por combinación de URL, usuario y contraseña.
 */
public class ConnectionPool implements AutoCloseable {

    /** Mapa de instancias de ConnectionPool  para implementar el patrón Singleton ampliado*/
    private static Map<Integer, ConnectionPool> instances = new HashMap<>();
    /** Fuente de datos de HikariCP */
    private final HikariDataSource ds;
    /** Número máximo de conexiones en el pool */
    public static short maxConnections = 10;
    /** Número mínimo de conexiones en el pool */
    public static short minConnections = 1;

    /**
     * Constructor privado para crear un pool de conexiones.
     * @param url URL de la base de datos.
     * @param user Usuario de conexión
     * @param password Contraseña de conexión
     */
    private ConnectionPool(String url, String user, String password) {
        HikariConfig hconfig = new HikariConfig();
        hconfig.setJdbcUrl(url);
        hconfig.setUsername(user);
        hconfig.setPassword(password);
        // Mínimo y máximo de conexiones.
        hconfig.setMaximumPoolSize(maxConnections);
        hconfig.setMinimumIdle(minConnections);
        ds = new HikariDataSource(hconfig);
    }

    /**
     * Genera un pool de conexiones o reaprovecha uno ya creado
     * si coinciden los parámetros de creación.
     * @param url URL de la base de datos.
     * @param user Usuario de conexión
     * @param password Contraseña de conexión
     * @return El pool de conexiones
     */
    public static ConnectionPool getInstance(String url, String user, String password) {
        int hashCode = Objects.hash(url, user, password);
        ConnectionPool instance = instances.get(hashCode);
        if(instance == null || instance.getDataSource().isClosed()) {
            instance = new ConnectionPool(url, user, password);
            instances.put(hashCode, instance);
        }
        return instance;
    }

    /**
     * Genera un pool de conexiones o reaprovecha uno ya creado
     * si ya se creo uno con la URL suministrada.
     * @param url La URL de conexión.
     * @return El pool de conexiones.
     */
    public static ConnectionPool getInstance(String url) {
        return ConnectionPool.getInstance(url, null, null);
    }

    /**
     * Devuelve un pool de conexiones cuando sólo hay un candidato posible.
     * Como efecto secundario, elimina los pools cuyo DataSource esté cerrado.
     * @return El pool de conexiones.
     */
    public static ConnectionPool getInstance() {
        ConnectionPool.clear();
        switch(instances.size()) {
            case 1:
                ConnectionPool instance = instances.values().iterator().next();
                if(instance.isActive()) return instance;
                else instances.clear();
            case 0:
                throw new IllegalArgumentException("No hay definido ningún pool activo");
            default:
                throw new IllegalArgumentException("Ambiguo: hay definidos varios pools");
        }
    }

    /**
     * Elimina los pools cuyos DataSource estén cerrados.
     */
    public static void clear() {
        instances = instances.entrySet()
            .stream().filter(e -> e.getValue().isActive())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Devuelve una conexión del pool.
     * @return Una conexión a la base de datos.
     * @throws SQLException Si ocurre un error al obtener la conexión.
     */
    public Connection getConnection() throws SQLException {
         return ds.getConnection();
    }

    /**
     * Devuelve la fuente de datos HikariCP asociada al pool de conexiones.
     * @return La fuente de datos HikariCP.
     */
    public HikariDataSource getDataSource() {
        return ds;
    }

    /**
     * Verifica si el pool de conexiones está activo.
     * @return true si el pool está activo, false si está cerrado.
     */
    public boolean isActive() {
        return !ds.isClosed();
    }

    /**
     * Cierra el pool de conexiones y libera los recursos asociados.
     */
    @Override
    public void close() {
        ds.close();
    }
}