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
    POSTGRESQL("jdbc:postgresql:[//<host>[:<port>]/]<base>", "org.postgresql.Driver"),
    ORACLE("jdbc:oracle:thin:@//[<host>[:<port>]/]<base>", "oracle.jdbc.OracleDriver"),
    MSSQL("jdbc:sqlserver://[<host>[:<port>];]databaseName=<base>", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    H2("jdbc:h2:[tcp://<host>[:<port>]/]<base>", "org.h2.Driver");

    private final String url;
    private final String driver;
    private Boolean supported;

    DbmsSelector(String url, String driver) {
        this.url = url;
        this.driver = driver;
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
        // Para Oracle y MSSQL siempre hay que expresar el host, aunque sea localhost.
        host = switch(this) {
            case ORACLE, MSSQL -> host == null ? "localhost" : host;
            default -> host;
        };

        return url
                .replace("<base>", base)
                .replace("[:<port>]", port != null ? ":" + port.toString() : "")
                .replaceFirst("\\[([^<]*)<host>([^\\]]*)\\]", (host != null ? "$1" + host + "$2" : (port == null ? "" : "$1" + "localhost" + "$2")));
    }

    /**
     * Devuelve el SGBD correspondiente a una cadena ("sqlite" devuelve SGBD.SQLITE).
     *
     * @param sgbd Cadena con el nombre del SGBD.
     * @return El SGBD correspondiente.
     * @throws IllegalArgumentException si el SGBD no es reconocido.
     * @throws UnsupportedOperationException si el SGBD no tiene driver disponible.
     */
    public static DbmsSelector fromString(String sgbd) {
        DbmsSelector dbms = Arrays.stream(DbmsSelector.values())
            .filter(v -> v.name().equalsIgnoreCase(sgbd))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(String.format("SGBD '%s' no reconocido.", sgbd)));

        if(!dbms.isSupported()) throw new UnsupportedOperationException(String.format("%s no tiene driver disponible. Debería añadir la dependencia correspondiente.", dbms.name()));

        return dbms;
        
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

    /**
     * Indica si el SGBD tiene driver disponible.
     * @return true, si así es. 
     */
    public boolean isSupported() {
        if(supported == null) {
            try {
                Class.forName(driver);
                supported = true;
            } catch (ClassNotFoundException e) {
                supported = false;
            }
        }
        return supported;
    }

    /**
     * Devuelve el driver JDBC asociado al SGBD.
     * @return El driver JDBC solicitado.
     */
    public String getDriver() {
        return driver;
    }
}