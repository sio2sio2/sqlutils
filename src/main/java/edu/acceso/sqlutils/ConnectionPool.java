package edu.acceso.sqlutils;

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
}