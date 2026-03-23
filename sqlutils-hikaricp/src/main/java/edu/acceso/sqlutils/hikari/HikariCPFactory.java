package edu.acceso.sqlutils.hikari;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import edu.acceso.sqlutils.DataSourceFactory;

public class HikariCPFactory implements DataSourceFactory {

    /** Número máximo de conexiones en el pool */
    private static short maxConnections = 10;
    /** Número mínimo de conexiones en el pool */
    private static short minConnections = 1;

    private final HikariConfig hconfig;

    /**
     * Constructor con configuración por defecto.
     */
    public HikariCPFactory() {
        hconfig = new HikariConfig();
        // Mínimo y máximo de conexiones.
        hconfig.setMaximumPoolSize(maxConnections);
        hconfig.setMinimumIdle(minConnections);
    }

    /**
     * Constructor con configuración personalizada.
     * @param config Configuración personalizada para el pool de conexiones.
     */
    public HikariCPFactory(HikariConfig config) {
        hconfig = config;
    }

    @Override
    public DataSource create(String dbUrl, String user, String password) {
        hconfig.setJdbcUrl(dbUrl);
        hconfig.setUsername(user);
        hconfig.setPassword(password);
        return new HikariDataSource(hconfig);
    }
}