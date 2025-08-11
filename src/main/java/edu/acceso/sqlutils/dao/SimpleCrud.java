package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.crud.SimpleCrudInterface;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.query.SqlQuery;
import edu.acceso.sqlutils.query.SqlTypesTranslator;

/** 
 * Clase que implementa las operaciones CRUD para una entidad genérica T.
 * @param <T> Tipo de entidad que extiende {@link Entity}.
 * 
 * <p>
 * Puede crearse a partir de una {@link DataSource} o una {@link Connection}.
 * En el primer caso, cada operación CRUD se ejecuta con una conexión nueva del pool.
 * En el segundo, se utiliza una conexión existente y todas las operaciones CRUD la comparten.
 * Esto es útil para transacciones que requieren múltiples operaciones CRUD.
 * </p>
 */
public class SimpleCrud<T extends Entity> extends AbstractCrud<T> implements SimpleCrudInterface<T> {
    /** Logger para registrar información y errores. */
    private static final Logger logger = LoggerFactory.getLogger(SimpleCrud.class);

    /**
     * Constructor que recibe un {@link DataSource} y una clase que implementa {@link SqlQuery}.
     * @param ds Una fuente de datos para obtener conexiones a la base de datos.
     * @param mapper El EntityMapper que mapea entidades a registros de la base de
     * @param daoF La fábrica de DAOs que se utilizará para obtener entidades relacionadas.
     */
    public SimpleCrud(DataSource ds, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                      Class<? extends SqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        super(ds, entityClass, mappers, sqlQueryClass, loaderClass);
    }

    /**
     * Constructor que recibe una {@link Connection} y una clase que implementa {@link SqlQuery}.
     * @param conn Una conexión a la base de datos.
     * @param mapper El EntityMapper que mapea entidades a registros de la base de
     * @param daoF La fábrica de DAOs que se utilizará para obtener entidades relacionadas.
     */
    public SimpleCrud(Connection conn, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                      Class<? extends SqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        super(conn, entityClass, mappers, sqlQueryClass, loaderClass);
    }

    /**
     * Constructor que crea una nueva instancia de {@link SimpleCrud} a partir de otro {@link SimpleCrud}.
     * 
     * <p>
     * Un {@link SimpleCrud} obtenido de este modo comparte el cargador de relaciones (véase
     * {@link RelationLoader}) con el original, lo que permite que éste conserve el historial
     * de todas las relaciones cargadas.
     * </p>
     * @param <E> Tipo de entidad del {@link SimpleCrud} original.
     * @param dao El {@link SimpleCrud} original a partir del cual se obtiene el nuevo.
     * @param entityClass La clase de la entidad que maneja el nuevo {@link SimpleCrud}.
     */
    public <E extends Entity> SimpleCrud(SimpleCrud<E> dao, Class<T> entityClass) {
        super(dao, entityClass);
    }

    @Override
    public Optional<T> get(Long id) throws DataAccessException {
        final String sql = sqlQuery.getSelectIdSql();

        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            return rs.next()
                ? Optional.of(mapper.resultSetToEntity(rs, loader))
                : Optional.empty();
        } catch (SQLException e) {
            logger.warn("Error al obtener el registro con ID {} de la tabla {}", id, mapper.getTableInfo().tableName(), e);
            throw new DataAccessException(String.format("Error al obtener el registro con ID %d", id), e);
        }
    }

    @Override
    public Stream<T> getStream() throws DataAccessException {
        final String sql = sqlQuery.getSelectSql();

        try {
            // RECURSOS: no se cierran, porque se espera que lo haga el consumidor del stream.
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            Stream<T> stream = SqlUtils.resultSetToStream(conn, pstmt, rs, fila -> mapper.resultSetToEntity(fila, loader));
            logger.info("Se ha obtenido un stream de registros de la tabla {}", mapper.getTableInfo().tableName());
            return stream;
        } catch (SQLException e) {
            logger.warn("Error al obtener el stream de registros de la tabla {}", mapper.getTableInfo().tableName(), e);
            throw new DataAccessException("Error al obtener el stream de registros", e);
        }
    }

    @Override
    public Stream<T> getStream(String field, Object value) throws DataAccessException {
        SqlTypesTranslator translator = (value instanceof Entity)
            ? new SqlTypesTranslator(Long.class, ((Entity) value).getId())
            : getTranslator(field, value);

        String column = mapper.getTableInfo().getColumnName(field);
        final String sql = sqlQuery.getSelectWhereSql(column);
        value = translator.getSqlValue();

        try {
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);

            pstmt.setObject(1, value, translator.getType());
            ResultSet rs = pstmt.executeQuery();

            Stream<T> stream = SqlUtils.resultSetToStream(conn, pstmt, rs, fila -> mapper.resultSetToEntity(fila, loader));
            logger.info("Se ha obtenido un stream de registros de la tabla {}", mapper.getTableInfo().tableName());
            return stream;
        } catch (SQLException e) {
            logger.warn("Error al obtener los registros de la tabla {} donde {} = {}", mapper.getTableInfo().tableName(), column, value, e);
            throw new DataAccessException(String.format("Error al obtener los registros de la tabla %s donde %s = %d", mapper.getTableInfo().tableName(), column, value), e);
        }
    }

    @Override
    public boolean delete(Long id) throws DataAccessException {
        final String sql = sqlQuery.getDeleteSql();

        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            pstmt.setLong(1, id);
            return pstmt.executeUpdate() > 0;
        } catch (SQLException e) {
            logger.warn("Error al eliminar el registro con ID {} de la tabla {}", id, mapper.getTableInfo().tableName(), e);
            throw new DataAccessException(String.format("Error al eliminar el registro con ID %d", id), e);
        }
    }

    @Override
    public void insert(T entity) throws DataAccessException {
        final String sql = sqlQuery.getInsertSql();

        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            mapper.EntityToParams(pstmt, entity);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            logger.warn("Error al insertar el registro con ID {} de la tabla {}", entity.getId(), mapper.getTableInfo().tableName(), e);
            throw new DataAccessException(String.format("Error al insertar el registro con ID %d", entity.getId()), e);
        }
    }

    @Override
    public boolean update(T entity) throws DataAccessException {
        final String sql = sqlQuery.getUpdateSql();

        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            mapper.EntityToParams(pstmt, entity);
            return pstmt.executeUpdate() > 0;
        } catch (SQLException e) {
            logger.warn("Error al actualizar el registro con ID {} de la tabla {}", entity.getId(), mapper.getTableInfo().tableName(), e);
            throw new DataAccessException(String.format("Error al actualizar el registro con ID %d", entity.getId()), e);
        }
    }

    @Override
    public boolean update(Long oldId, Long newId) throws DataAccessException {
        final String sql = sqlQuery.getUpdateIdSql();

        try(
            Connection conn = cp.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            pstmt.setLong(1, newId);
            pstmt.setLong(2, oldId);
            return pstmt.executeUpdate() > 0;
        } catch (SQLException e) {
            logger.warn("Error al actualizar el ID del registro de {} a {} en la tabla {}", oldId, newId, mapper.getTableInfo().tableName(), e);
            throw new DataAccessException(String.format("Error al actualizar el ID del registro de %d a %d", oldId, newId), e);
        }
    }
}