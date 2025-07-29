package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.crud.Crud;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.LoaderFactory;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.dao.tx.TransactionContext;
import edu.acceso.sqlutils.dao.tx.TransactionalOperation;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.query.SqlQuery;

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
    final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers;
    /** Fuente de datos utilizada para obtener conexiones a la base de datos. */
    final DataSource ds;
    /** Clase que implementa las sentencias SQL para las operaciones CRUD. */
    final Class<? extends SqlQuery> sqlQueryClass;
    /** Clase que implementa el cargador de relaciones. */
    final Class<? extends RelationLoader> loaderClass;

    /**
     * Constructor que recibe una fuente de datos, una clase que implementa {@link SqlQuery} y un {@link LoaderFactory}.
     * @param ds Una fuente de datos para obtener conexiones a la base de datos.
     * @param sqlQueryClass La clase que implementa las sentencias SQL para las operaciones CRUD.
     * @param loader La fábrica de cargadores de relaciones.
     */
    private DaoFactory(DataSource ds, Class<? extends SqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass, Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers) {
        this.ds = ds;
        this.sqlQueryClass = sqlQueryClass;
        this.loaderClass = loaderClass;
        this.mappers = mappers;
    }

    public static class Builder {
        private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers = new HashMap<>();
        private final Class<? extends SqlQuery> sqlQueryClass;

        private Builder(Class<? extends SqlQuery> sqlQueryClass) {
            this.sqlQueryClass = sqlQueryClass;
        }

        public static Builder create(Class<? extends SqlQuery> sqlQueryClass) {
            return new Builder(sqlQueryClass);
        }

        /**
         * Registra un {@link EntityMapper} para una entidad específica.
         * @param entityMapperClass La clase del objeto {@link EntityMapper} que se va a registrar.
         * @return La propia instancia de {@link DaoFactory} para permitir el encadenamiento de llamadas.
         */
        public Builder registerMapper(Class<? extends EntityMapper<? extends Entity>> entityMapperClass) {
            Class<? extends Entity> entityClass = EntityMapper.getEntityType(entityMapperClass);
            EntityMapper<? extends Entity> mapper = null;
            try {
                mapper = entityMapperClass.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Error al crear instancia de EntityMapper", e);
            }
            mappers.put(entityClass, mapper);
            return this;
        }

        public DaoFactory get(DataSource ds, LoaderFactory loader) {
            return new DaoFactory(ds, sqlQueryClass, loader.getLoaderClass(), mappers);
        }
    }

    /**
     * Obtiene un objeto {@link Crud} que permite realizar operaciones CRUD sobre una entidad específica.
     * @param entityClass La clase de la entidad para la que se desea obtener el DAO.
     * @return Un objeto {@link Crud} para la entidad especificada.
     */
    public <T extends Entity> Crud<T> getDao(Class<T> entityClass) {
        return new DaoCrud<>(ds, entityClass, this);
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
                operations.accept(new TransactionContext(conn, this));
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
