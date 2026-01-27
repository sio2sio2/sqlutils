package edu.acceso.sqlutils.dao;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.dao.crud.DaoProvider;
import edu.acceso.sqlutils.dao.crud.simple.SimpleCrudInterface;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.LoaderFactory;
import edu.acceso.sqlutils.dao.relations.RelationLoader;

/** 
 * Fábrica de DAOs que permite crear objetos DAO para realizar operaciones CRUD
 * sobre entidades. Esta clase es responsable de registrar los mappers de entidades y
 * proporcionar acceso a ellas a través del método {@link #getDao(Class)}.
 * 
 */
public class DaoFactory {
    /** Logger para registrar información y errores. */
    private static final Logger logger = LoggerFactory.getLogger(DaoFactory.class);

    /** Mapa que relaciona las clases de entidad con sus mappers. */
    private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers;
    /** Clave que identifica la fuente de datos. */
    private final String key;
    /** Proveedor de DAOs (que proporciona una implementación de las operaciones CRUD y una definición de las consultas SQL) */
    private final DaoProvider daoProvider;
    /** Clase que implementa el cargador de relaciones. */
    private final Class<? extends RelationLoader> loaderClass;

    /**
     * Constructor privado para la fábrica de DAOs.
     * @param key Clave que identifica la fuente de datos.
     * @param daoProvider Proveedor de DAOs 
     * @param loaderClass Clase que implementa el cargador de relaciones.
     * @param mappers Mapa de mappers de entidades.
     */
    private DaoFactory(String key, DaoProvider daoProvider, Class<? extends RelationLoader> loaderClass, Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers) {
        this.key = key;
        this.daoProvider = daoProvider;
        this.loaderClass = loaderClass;
        this.mappers = mappers;
    }

    /**
     * Clase que permite construir instancias de {@link DaoFactory}.
     */
    public static class Builder {
        /**
         * Mapa de mappers de entidades.
         */
        private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers = new HashMap<>();
        /**
         * Proveedor de DAOs.
         */
        private final DaoProvider daoProvider;

        /**
         * Constructor privado para la clase Builder.
         * @param daoProvider Proveedor de DAOs.
         */
        private Builder(DaoProvider daoProvider) {
            this.daoProvider = daoProvider;
        }

        /**
         * Crea una nueva instancia de {@link DaoFactory.Builder}.
         * @param daoProvider Proveedor de DAOs.
         * @return Una nueva instancia de {@link DaoFactory.Builder}.
         */
        public static Builder create(DaoProvider daoProvider) {
            return new Builder(daoProvider);
        }

        /**
         * Registra un {@link EntityMapper} para una entidad específica.
         * @param entityMapperClass La clase del objeto {@link EntityMapper} que se va a registrar.
         * @return La propia instancia de {@link DaoFactory} para permitir el encadenamiento de llamadas.
         */
        public <T extends Entity> Builder registerMapper(Class<? extends EntityMapper<T>> entityMapperClass) {
            Class<T> entityClass = EntityMapper.getEntityType(entityMapperClass);
            EntityMapper<T> mapper = null;
            try {
                mapper = entityMapperClass.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Error al crear instancia de EntityMapper", e);
            }
            mappers.put(entityClass, mapper);
            return this;
        }

        /**
         * Genera una nueva instancia de {@link DaoFactory}.
         * @param ds Fuente de datos para obtener conexiones a la base de datos.
         * @param loader Cargador de relaciones.
         * @return Una nueva instancia de {@link DaoFactory}.
         */
        public DaoFactory get(String key, LoaderFactory loader) {
            return new DaoFactory(key, daoProvider, loader.getLoaderClass(), mappers);
        }
    }

    /**
     * Obtiene un objeto {@link SimpleCrudInterface} que permite realizar operaciones CRUD sobre una entidad específica.
     * @param entityClass La clase de la entidad para la que se desea obtener el DAO.
     * @param <T> El tipo de la entidad.
     * @return Un objeto DAO para la entidad especificada.
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> AbstractCrud<T> getDao(Class<T> entityClass) {
        try {
            return (AbstractCrud<T>) daoProvider.getCrudClass().getConstructor(String.class, Class.class, Map.class, Class.class, Class.class)
                    .newInstance(key, entityClass, mappers, daoProvider.getSqlQueryClass(), loaderClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(String.format("Error al crear la instancia de %s", daoProvider.getCrudClass().getSimpleName()), e);
        }
    }
}