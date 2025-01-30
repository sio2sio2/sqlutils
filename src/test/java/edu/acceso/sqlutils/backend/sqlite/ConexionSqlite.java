package edu.acceso.sqlutils.backend.sqlite;

import java.nio.file.Path;
import java.util.Map;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import edu.acceso.sqlutils.dao.DaoConnection;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Modela la conexión a una base de dato SQLite
 */
public class ConexionSqlite extends DaoConnection {
    final static String protocol = "jdbc:sqlite:";
    final static short maxConn = 10;
    final static short minConn = 1;

    /**
     * Constructor de la conexión.
     * Si la url+username+password coincide con una que ya se haya utilizado, no se crea un objeto
     * distinto, sino que se devuelve el objeto que se creó anteriormente.
     * @param opciones Las opciones de conexión.
     * @param scriptSql El archivo con el guión SQL que crea el esquema en la base de datos.
     * @param daoClasses Las clases DAO que se usarán con la conexión para hacer persistentes los objetos.
     */
    public ConexionSqlite(Map<String, Object> opciones, Path scriptSql, Class<?> ... daoClasses) throws DataAccessException {
        super(opciones, scriptSql, daoClasses);
    }

    // No he implementados los otros dos constructores posibles, porque no los usaré.

    /**
     * Crea el pool de conexiones.
     * @param opciones Las opciones para crear el pool.
     */
    @Override
    protected DataSource createDataSource(Map<String, Object> opciones) {
        String path = (String) opciones.get("url");
        if(path == null) throw new IllegalArgumentException("No se ha fijado la url de la base de datos");

        String dbUrl = String.format("%s%s", protocol, path);
        Short maxConn = (Short) opciones.getOrDefault("maxconn", ConexionSqlite.maxConn);
        Short minConn = (Short) opciones.getOrDefault("minconn", ConexionSqlite.minConn);


        HikariConfig hconfig = new HikariConfig();
        hconfig.setJdbcUrl(dbUrl);
        hconfig.setMaximumPoolSize(maxConn);
        hconfig.setMinimumIdle(minConn);

        return new HikariDataSource(hconfig);
    }

    /**
     * Define cómo se diferencias unas conexiones de otras.
     * En el caso de SQLite, sólo por la URL, ya que no hay credenciales.
     * @param opciones Las opciones de conexión.
     */
    @Override
    protected String generateKey(Map<String, Object> opciones) {
        return (String) opciones.get("url");
    }
}