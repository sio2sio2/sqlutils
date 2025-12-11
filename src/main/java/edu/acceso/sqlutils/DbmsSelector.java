package edu.acceso.sqlutils;

import java.util.Arrays;

/**
 * Enumeración de Sistemas de Gestión de Bases de Datos (SGBD). Permite obtener
 * la URL de conexión JDBC correspondiente a cada SGBD ({@link #getUrl(String, String, Integer)}), así como buscar un SGBD
 * a partir de su nombre ({@link #fromString(String)}) o de una URL de conexión JDBC ({@link #fromUrl(String)}).
 */
public enum DbmsSelector {

    // SGBD con los patrones de URL de conexión JDBC y el driver correspondiente
    SQLITE("jdbc:sqlite:<base>", "org.sqlite.JDBC"),
    MARIADB("jdbc:mariadb://[<host>[:<port>]]/<base>", "org.mariadb.jdbc.Driver"),
    MYSQL("jdbc:mysql://[<host>[:<port>]]/<base>", "com.mysql.cj.jdbc.Driver"),
    POSTGRESQL("jdbc:postgresql://[<host>[:<port>]]/<base>", "org.postgresql.Driver"),
    ORACLE("jdbc:oracle:thin:@//[<host>[:<port>]]/<base>", "oracle.jdbc.OracleDriver"),
    MSSQL("jdbc:sqlserver://[<host>[:<port>];]databaseName=<base>", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    H2("jdbc:h2:tcp://[<host>[:<port>]]/<base>", "org.h2.Driver");

    private final String url;
    private final String driverClass;
    private Boolean supported;

    DbmsSelector(String url, String driverClass) {
        this.url = url;
        this.driverClass = driverClass;
    }

    /**
     * Obtiene la URL de conexión JDBC para el SGBD, reemplazando los marcadores
     * de posición por los valores indicados.
     *
     * @param base Nombre de la base de datos
     * @param host Nombre del host (puede ser null para localhost)
     * @param port Puerto (puede ser null para el puerto por defecto)
     * @return URL de conexión JDBC
     */
    public String getUrl(String base, String host, Integer port) {
        return url
                .replace("<base>", base)
                .replace("[:<port>]", port != null ? ":" + port.toString() : "")
                .replaceFirst("\\[<host>([^\\]]*)\\]", (host != null ? host : "localhost") + "$1");
    }

    /**
     * Devuelve el SGBD correspondiente a una cadena ("sqlite" devuelve SGBD.SQLITE).
     *
     * @param sgbd Cadena con el nombre del SGBD.
     * @param onlySupported true para buscar sólo entre los SGBD soportados.
     * @return SGBD correspondiente, o null si no se encuentra
     */
    public static DbmsSelector fromString(String sgbd, boolean onlySupported) {
        return Arrays.stream(DbmsSelector.values())
                .filter(v -> v.name().equalsIgnoreCase(sgbd) && (!onlySupported || v.isSupported()))
                .findFirst()
                .orElse(null);
    }

    /**
     * Devuelve el SGBD correspondiente a una cadena ("sqlite" devuelve SGBD.SQLITE).
     * @param sgbd Cadena con el nombre del SGBD.
     * @return SGBD correspondiente, o null si no se encuentra.
     */
    public static DbmsSelector fromString(String sgbd) {
        return fromString(sgbd, false);
    }

    /**
     * Obtiene el SGBD correspondiente a una URL de conexión JDBC.
     *
     * @param url URL de conexión JDBC
     * @return SGBD correspondiente, o null si no se encuentra
     */
    public static DbmsSelector fromUrl(String url) {
        return Arrays.stream(DbmsSelector.values())
                .filter(v -> url.startsWith(v.url.substring(0, v.url.indexOf("<")).replace("[", "")))
                .findFirst()
                .orElse(null);
    }

    public boolean isSupported() {
        if(supported == null) {
            try {
                Class.forName(driverClass);
                supported = true;
            } catch (ClassNotFoundException e) {
                supported = false;
            }
        }
        return supported;
    }
}