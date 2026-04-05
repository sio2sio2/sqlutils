package edu.acceso.sqlutils.jdbc;

import javax.sql.DataSource;

/**
 * Interfaz para la creación de DataSource. Permite definir diferentes
 * maneras de crear el objeto DataSource necesario para establecer conexiones a la base de datos
 * (HikariCP, Tomcat, Apache, etc.).
 * En el módulo sqlutils-hikaricp se proporciona una implementación de esta interfaz que utiliza
 * la librería HikariCP.
 */
public interface DataSourceFactory {
    /**
     * Crea un DataSource a partir de los datos de conexión.
     * @param dbUrl URL de conexión a la base de datos.
     * @param user Usuario de conexión
     * @param password Contraseña de conexión
     * @return El DataSource creado.
     */
    public DataSource create(String dbUrl, String user, String password);
}