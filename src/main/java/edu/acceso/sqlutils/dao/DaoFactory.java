package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.dao.crud.DaoProvider;
import edu.acceso.sqlutils.dao.crud.simple.SimpleCrudInterface;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.LoaderFactory;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.dao.tx.TransactionContext;
import edu.acceso.sqlutils.dao.tx.TransactionalOperation;
import edu.acceso.sqlutils.errors.DataAccessException;

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
    /** Fuente de datos utilizada para obtener conexiones a la base de datos. */
    private final DataSource ds;
    /** Proveedor de DAOs (que proporciona una implementación de las operaciones CRUD y una definición de las consultas SQL) */
    private final DaoProvider daoProvider;
    /** Clase que implementa el cargador de relaciones. */
    private final Class<? extends RelationLoader> loaderClass;

    /**
     * Constructor privado para la fábrica de DAOs.
     * @param ds Fuente de datos para obtener conexiones a la base de datos.
     * @param daoProvider Proveedor de DAOs 
     * @param loaderClass Clase que implementa el cargador de relaciones.
     * @param mappers Mapa de mappers de entidades.
     */
    private DaoFactory(DataSource ds, DaoProvider daoProvider, Class<? extends RelationLoader> loaderClass, Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers) {
        this.ds = ds;
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
        public DaoFactory get(DataSource ds, LoaderFactory loader) {
            return new DaoFactory(ds, daoProvider, loader.getLoaderClass(), mappers);
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
            return (AbstractCrud<T>) daoProvider.getCrudClass().getConstructor(DataSource.class, Class.class, Map.class, Class.class, Class.class)
                    .newInstance(ds, entityClass, mappers, daoProvider.getSqlQueryClass(), loaderClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(String.format("Error al crear la instancia de %s", daoProvider.getCrudClass().getSimpleName()), e);
        }
    }

    /**
     * Realiza una transacción utilizando un {@link TransactionalOperation} que recibe un {@link DaoFactory}.
     * Permite ejecutar operaciones de una o varias entidades dentro de una misma transacción.
     *
     * @param operations El consumidor que permite definir las operaciones a realizar dentro de la transacción.
     * @throws DataAccessException Si ocurre un error al realizar la transacción.
     */
    public void transaction(TransactionalOperation operations) throws DataAccessException {
        try {
            Connection conn = ds.getConnection();
            try {
                conn.setAutoCommit(false);
                logger.debug("Se abre transacción");
                operations.accept(new TransactionContext(conn, daoProvider, loaderClass, mappers));
                conn.commit();
            } catch (DataAccessException | SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException ex) {
                    logger.warn("Error al realizar el rollback", ex);
                }
                throw new DataAccessException("Error en la transacción", e);
            } finally {
                logger.debug("Se cierra transacción");
                conn.close();
            }
        } catch (SQLException e) {
            throw new DataAccessException("Error al obtener la conexión para la transacción", e);
        }
    }
}