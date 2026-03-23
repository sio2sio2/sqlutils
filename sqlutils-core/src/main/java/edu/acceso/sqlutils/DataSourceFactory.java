package edu.acceso.sqlutils;

import javax.sql.DataSource;

/**
 * Interfaz para la creación de DataSource. Permite definir diferentes
 * maneras de crear el objeto DataSource necesario para establecer conexiones a la base de datos
 * (HikariCP, Tomcat, Apache, etc.).
 */
public interface DataSourceFactory {
    public DataSource create(String dbUrl, String user, String password);
}