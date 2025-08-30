package edu.acceso.sqlutils.dao.crud;

import java.util.HashMap;
import java.util.Map;

import edu.acceso.sqlutils.dao.crud.simple.SimpleSqlQuery;

/**
 * Fabrica de consultas SQL. Se utiliza un patrón Singleton ampliado para soporta
 * el poder crearse distintos juegos de instancias. Dentro de cada juego las instancias
 * SqlQuery se asocian a los prefijos de la URL de conexión a la base de datos.
 * El prefijo genérico "*" se utiliza si no se encuentra el prefijo específico.
 */
public class SqlQueryFactory {

    /** Instancias de la fábrica. */
    private static Map<String, SqlQueryFactory> instances = new HashMap<>();
    /** Mapeador de prefijos a clases de consulta SQL. */
    private Map<String, Class<? extends SimpleSqlQuery>> mapper;

    /**
     * Constructor privado para evitar instanciación externa.
     * @param name Nombre de la instancia única.
     * @param mapper Mapa de prefijos a clases de SqlQuery.
     */
    private SqlQueryFactory(String name, Map<String, Class<? extends SimpleSqlQuery>> mapper) {
        this.mapper = mapper;
        instances.put(name, this);
    }

    /**
     * Clase Builder para construir la instancia de SqlQueryFactory.
     * Permite registrar clases de SqlQuery asociadas a prefijos específicos.
     */
    public static class Builder {

        /** Nombre de la instancia única de {@link SqlQueryFactory}. */
        private String name;
        /** Mapa de prefijos a clases de SqlQuery. */
        private final Map<String, Class<? extends SimpleSqlQuery>> mapper = new HashMap<>();

        private Builder(String name) {
            this.name = name;
         }

        /**
         * Método estático para iniciar la construcción de SqlQueryFactory.
         * @param name Nombre de la instancia única.
         * @return Un nuevo Builder.
         */
        public static Builder create(String name) {
            return new Builder(name);
        }

        /**
         * Registra una clase SqlQuery con un prefijo específico.
         * @param prefix Prefijo del URL de conexión.
         * @param sqlQueryClass Clase que implementa SqlQuery.
         * @return El Builder para encadenar llamadas.
         */
        public Builder register(String prefix, Class<? extends SimpleSqlQuery> sqlQueryClass) {
            mapper.put(prefix, sqlQueryClass);
            return this;
        }

        /**
         * Construye la instancia de SqlQueryFactory.
         * @return La instancia de SqlQueryFactory.
         */
        public SqlQueryFactory get() {
            return SqlQueryFactory.create(name, mapper);
        }
    }

    /**
     * Método para crear la instancia de SqlQueryFactory.
     * @param name Nombre de la instancia de {@link SqlQueryFactory}.
     * @param mapper Mapa de prefijos a clases de SqlQuery.
     * @return La instancia de SqlQueryFactory.
     */
    private static synchronized SqlQueryFactory create(String name, Map<String, Class<? extends SimpleSqlQuery>> mapper) {
        if (mapper.containsKey(name)) {
            throw new IllegalStateException(String.format("SqlQueryFactory con nombre '%s' ya fue inicializado.", name));
        }
        return new SqlQueryFactory(name, mapper);
    }

    /**
     * Obtiene la instancia de SqlQueryFactory por nombre.
     * @param name Nombre de la instancia de {@link SqlQueryFactory}.
     * @return La instancia de SqlQueryFactory.
     * @throws IllegalStateException Si la instancia no ha sido inicializada.
     */
    public static SqlQueryFactory getInstance(String name) {
        SqlQueryFactory instance = instances.get(name);
        if (instance == null) {
            throw new IllegalStateException(String.format("SqlQueryFactory con nombre '%s' no se ha inicializado.", name));
        }
        return instance;
    }

    /**
     * Obtiene la instancia única de SqlQueryFactory.
     * @return La instancia de SqlQueryFactory.
     * @throws IllegalStateException Si la instancia no ha sido inicializada o existen varias.
     */
    public static SqlQueryFactory getInstance() {
        return switch(instances.size()) {
            case 0 -> throw new IllegalStateException("SqlQueryFactory no ha sido inicializado.");
            case 1 -> instances.values().iterator().next();
            default -> throw new IllegalStateException("SqlQueryFactory tiene múltiples instancias. Use getInstance(String name).");
        };
    }

    /**
     * Crea una instancia de SqlQuery basada en la URL proporcionada.
     * @param url La URL de conexión a la base de datos.
     * @return La clase SqlQuery correspondiente.
     */
    public Class<? extends SimpleSqlQuery> createSqlQuery(String url) {
        if (url.startsWith("jdbc:")) {
            url = url.substring(5);
        }
        String prefix = url.substring(0, url.indexOf(":"));

        // Se prueba a buscar el prefijo del SGBD y, si no existe, se utiliza el genérico "*".
        Class<? extends SimpleSqlQuery> sqlQueryClass = mapper.getOrDefault(prefix, mapper.get("*"));
        if (sqlQueryClass == null) {
            throw new UnsupportedOperationException("SGBD no soportado: " + prefix);
        }
        return sqlQueryClass;
    }
}