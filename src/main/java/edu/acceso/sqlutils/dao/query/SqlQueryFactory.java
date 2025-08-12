package edu.acceso.sqlutils.dao.query;

import java.util.HashMap;
import java.util.Map;

/**
 * SqlQueryFactory es un singleton que permite crear instancias de SqlQuery
 * basadas en el prefijo del URL de conexión a la base de datos.
 */
public class SqlQueryFactory {

    /** Instancia única de la fábrica. */
    private static SqlQueryFactory instance;
    /** Mapeador de prefijos a clases de consulta SQL. */
    private Map<String, Class<? extends SqlQuery>> mapper;

    /**
     * Constructor privado para evitar instanciación externa.
     * @param mapper Mapa de prefijos a clases de SqlQuery.
     */
    private SqlQueryFactory(Map<String, Class<? extends SqlQuery>> mapper) {
        this.mapper = mapper;
    }

    /**
     * Clase Builder para construir la instancia de SqlQueryFactory.
     * Permite registrar clases de SqlQuery asociadas a prefijos específicos.
     */
    public static class Builder {

        /** Mapa de prefijos a clases de SqlQuery. */
        private final Map<String, Class<? extends SqlQuery>> mapper = new HashMap<>();

        private Builder() { }

        /**
         * Método estático para iniciar la construcción de SqlQueryFactory.
         * @return Un nuevo Builder.
         */
        public static Builder create() {
            return new Builder();
        }

        /**
         * Registra una clase SqlQuery con un prefijo específico.
         * @param name Prefijo del URL de conexión.
         * @param sqlQueryClass Clase que implementa SqlQuery.
         * @return El Builder para encadenar llamadas.
         */
        public Builder register(String name, Class<? extends SqlQuery> sqlQueryClass) {
            mapper.put(name, sqlQueryClass);
            return this;
        }

        /**
         * Construye la instancia de SqlQueryFactory.
         * @return La instancia de SqlQueryFactory.
         */
        public SqlQueryFactory get() {
            return SqlQueryFactory.create(mapper);
        }
    }

    /**
     * Método para crear la instancia de SqlQueryFactory.
     * @param mapper Mapa de prefijos a clases de SqlQuery.
     * @return La instancia de SqlQueryFactory.
     */
    private static synchronized SqlQueryFactory create(Map<String, Class<? extends SqlQuery>> mapper) {
        if (instance != null) {
            throw new IllegalStateException("SqlQueryFactory ya fue inicializado.");
        }
        instance = new SqlQueryFactory(mapper);
        return instance;
    }

    /**
     * Obtiene la instancia única de SqlQueryFactory.
     * @return La instancia de SqlQueryFactory.
     * @throws IllegalStateException Si la instancia no ha sido inicializada.
     */
    public static SqlQueryFactory getInstance() {
        if (instance == null) {
            throw new IllegalStateException("SqlQueryFactory no se ha inicializado.");
        }
        return instance;
    }

    /**
     * Crea una instancia de SqlQuery basada en la URL proporcionada.
     * @param url La URL de conexión a la base de datos.
     * @return La clase SqlQuery correspondiente.
     */
    public Class<? extends SqlQuery> createSqlQuery(String url) {
        if (url.startsWith("jdbc:")) {
            url = url.substring(5);
        }
        String prefix = url.substring(0, url.indexOf(":"));

        Class<? extends SqlQuery> sqlQueryClass = mapper.get(prefix);
        if (sqlQueryClass == null) {
            throw new UnsupportedOperationException("SGBD no soportado: " + prefix);
        }
        return sqlQueryClass;
    }
}
