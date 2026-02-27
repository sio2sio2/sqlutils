package edu.acceso.sqlutils.orm;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.DbmsSelector;
import edu.acceso.sqlutils.orm.mapper.EntityMapper;
import edu.acceso.sqlutils.orm.minimal.Entity;
import edu.acceso.sqlutils.orm.minimal.sql.MinimalSqlQuery;

/**
 * Fabrica de consultas SQL que permite asociar las distintas implementaciones
 * que extienden {@link MinimalSqlQuery} a sus respectivos SGBD. En cualquier
 * caso, todas estan implementaciónes debe cumplir una misma interfaz extendida
 * de {@link MinimalSqlQuery} por lo que el Builder es genérico y se asegura
 * de que no puedan registrarse clases que implementen distintas interfaces.
 */
public class SqlQueryFactory {
    private static final Logger logger = LoggerFactory.getLogger(SqlQueryFactory.class);

    /** Mapeador de un determinado SGBD a clases de consulta SQL. */
    private final Class<? extends MinimalSqlQuery> sqlQueryClass;

    private final Map<EntityMapper<? extends Entity>, MinimalSqlQuery> cache = new ConcurrentHashMap<>();

    /**
     * Constructor privado para evitar instanciación externa.
     * @param sqlQueryClass Clase de SqlQuery a usar.
     */
    private SqlQueryFactory(Class<? extends MinimalSqlQuery> sqlQueryClass) {
        this.sqlQueryClass = sqlQueryClass;
        logger.debug("Creada SqlQueryFactory para clase de SqlQuery '{}'", sqlQueryClass.getSimpleName());
    }

    /**
     * Clase Builder para construir la instancia de SqlQueryFactory.
     * Permite registrar cómo se escribes las mismas sentencias SQL para distintos SGBD.
     * El Builder es genérico para asegurar que todas las clases registradas
     * implementen la misma interfaz de consultas SQL.
     * @param <S> Tipo de consultas SQL que extiende MinimalSqlQuery.
     */
    public static class Builder<S extends MinimalSqlQuery> {

        /** Mapa de SGBDs a clases de SqlQuery. */
        private final Map<DbmsSelector, Class<? extends S>> mapper = new HashMap<>();

        private Builder() {
            super();
        }

        /**
         * Método estático para iniciar la construcción de SqlQueryFactory.
         * @param <T> Tipo de consultas SQL que extiende MinimalSqlQuery.
         * @param defaultSqlQueryClass Clase de SqlQuery que deben extender todas las clases registradas.
         * @return Un nuevo Builder.
         */
        public static <T extends MinimalSqlQuery> Builder<T> create(Class<T> defaultSqlQueryClass) {
            return new Builder<T>();
        }

        /**
         * Registra una clase de consultas SQL para un {@link DbmsSelector} específico o genérico.
         * @param dbms SGBD para el cual se registra la clase. Utilice null, si quiere registrar una clase genérica para cualquier SGBD.
         * @param sqlQueryClass Clase que implementa SqlQuery.
         * @return El propio Builder para encadenar llamadas.
         */
        public Builder<S> register(DbmsSelector dbms, Class<? extends S> sqlQueryClass) {
            mapper.put(dbms, sqlQueryClass);
            logger.trace("Registrada clase '{}' para el SGBD '{}'", sqlQueryClass.getSimpleName(), dbms);
            return this;
        }

        /**
         * Registra una clase de consultas SQL para un nombre de SGBD específico o genérico.
         * @param dbmsName Nombre del SGBD tal como aparece en {@link DbmsSelector}. Utilice "*" para registrar una clase genérica para cualquier SGBD.
         * @param sqlQueryClass Clase que implementa SqlQuery.
         * @return El propio Builder para encadenar llamadas.
         */
        public Builder<S> register(String dbmsName, Class<? extends S> sqlQueryClass) {
            DbmsSelector dbms = dbmsName.equals("*") ? null : DbmsSelector.fromString(dbmsName);
            return register(dbms, sqlQueryClass);
        }

        /**
         * Construye la instancia de SqlQueryFactory.
         * @param dbms SGBD para el cual se desea obtener la fábrica de consultas SQL.
         *  Si no se encuentra una clase específica para ese SGBD, se usará la clase genérica registrada con null o "*".
         * @return La instancia de SqlQueryFactory.
         */
        public SqlQueryFactory get(DbmsSelector dbms) {
            return new SqlQueryFactory(mapper.getOrDefault(dbms, mapper.get(null)));
        }
    }

    /**
     * Crea una instancia de SqlQuery a partir del SGBD y el EntityMapper proporcionados.
     * @param <S> Tipo de consultas SQL que extiende MinimalSqlQuery.
     * @param <E> Tipo de entidad que mapea el EntityMapper.
     * @param entityMapper El EntityMapper que contiene la información de la tabla.
     * @return La clase SqlQuery correspondiente.
     */
    @SuppressWarnings("unchecked")
    public <S extends MinimalSqlQuery, E extends Entity> S createSqlQuery(EntityMapper<E> entityMapper) {
        return (S) cache.computeIfAbsent(entityMapper, em -> {
            try {
                var sqlQuery = sqlQueryClass.getConstructor(String.class, String.class, String[].class).newInstance(
                    em.getTableInfo().tableName(),
                    em.getTableInfo().idColumn().getName(),
                    em.getTableInfo().getColumnNames()
                );

                logger.atTrace()
                    .setMessage("Creada SqlQuery de clase '{}' para la entidad '{}'")
                    .addArgument(sqlQueryClass.getSimpleName())
                    .addArgument(() -> EntityMapper.getEntityType((Class<? extends EntityMapper<E>>) entityMapper.getClass()))
                    .log();

                return sqlQuery;
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Error al crear instancia de SqlQuery para el EntityMapper: " + em, e);
            }
        });
    }
}