package edu.acceso.sqlutils.persistence.jpa;

import java.util.Arrays;
import java.util.Map;

import edu.acceso.sqlutils.DbmsSelector;

/**
 * Enum que representa los proveedores JPA soportados por la aplicación, junto con sus clases de proveedor y dialectos asociados.
 */
public enum JpaProvider {
    HIBERNATE(
        "org.hibernate.jpa.HibernatePersistenceProvider",
        "hibernate.dialect",
        Map.of(
            DbmsSelector.MARIADB, "org.hibernate.dialect.MariaDBDialect",
            DbmsSelector.MYSQL, "org.hibernate.dialect.MySQLDialect",
            DbmsSelector.POSTGRESQL, "org.hibernate.dialect.PostgreSQLDialect",
            DbmsSelector.SQLITE, "org.hibernate.community.dialect.SQLiteDialect",
            DbmsSelector.ORACLE, "org.hibernate.dialect.OracleDialect",
            DbmsSelector.MSSQL, "org.hibernate.dialect.SQLServerDialect",
            DbmsSelector.H2, "org.hibernate.dialect.H2Dialect"
        )
    );

    private final String providerClass;
    private final String dialectKey;
    private final Map<DbmsSelector, String> dialects;
    private DbmsSelector sgbd;

    JpaProvider(String providerClass, String dialectKey, Map<DbmsSelector, String> dialects) {
        this.providerClass = providerClass;
        this.dialectKey = dialectKey;
        this.dialects = dialects;
    }

    public JpaProvider withSgbd(DbmsSelector sgbd) {
        this.sgbd = sgbd;
        return this;
    }

    public String getProviderClass() {
        return providerClass;
    }

    public String getDialectKey() {
        return dialectKey;
    }

    public static JpaProvider of(String provider) {
        return Arrays.stream(values())
                .filter(p -> p.name().equalsIgnoreCase(provider))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Proveedor JPA desconocido o no soportado: " + provider)); 
    }

    /**
     * Devuelve el dialecto de proveeder para la conexión a la base de datos.
     * @return El dialecto de proveedor solicitado, o {@code null} si no se ha podido determinar.
     */
    public String getDialect() {
        return dialects.get(sgbd);
    }
}
