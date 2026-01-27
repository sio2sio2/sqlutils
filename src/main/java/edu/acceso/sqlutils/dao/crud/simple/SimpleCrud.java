package edu.acceso.sqlutils.dao.crud.simple;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.mapper.SqlTypesTranslator;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.errors.DataAccessException;

/** 
 * Clase que implementa las operaciones CRUD para una entidad genérica T.
 * @param <T> Tipo de entidad que extiende {@link Entity}.
 */
public class SimpleCrud<T extends Entity> extends AbstractCrud<T> implements SimpleCrudInterface<T> {
    /** Logger para registrar información y errores. */
    private static final Logger logger = LoggerFactory.getLogger(SimpleCrud.class);

    /**
     * Constructor que recibe una clave identificativa y una clase que implementa {@link SimpleSqlQuery}.
     * @param key Clave que identifica la fuente de datos.
     * @param entityClass Clase de la entidad que maneja el DAO.
     * @param mappers Mapa que relaciona las entidades con sus respectivos {@link EntityMapper}.
     * @param sqlQueryClass Clase que implementa {@link SimpleSqlQuery}.
     * @param loaderClass Clase que implementa {@link RelationLoader}.
     */
    public SimpleCrud(String key, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                      Class<? extends SimpleSqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        super(key, entityClass, mappers, sqlQueryClass, loaderClass);
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

    private SimpleSqlQuery getSqlQuery() {
        return (SimpleSqlQuery) sqlQuery;
    }

    @Override
    public Optional<T> get(Long id) throws DataAccessException {
        final String sql = sqlQuery.getSelectIdSql();

        try(
            Connection conn = tm.getConnection();
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
        final String sql = getSqlQuery().getSelectSql();

        try {
            // RECURSOS: no se cierran, porque se espera que lo haga el consumidor del stream.
            Connection conn = tm.getConnection();
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
        final String sql = getSqlQuery().getSelectWhereSql(column);
        value = translator.getSqlValue();

        try {
            Connection conn = tm.getConnection();
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
        final String sql = getSqlQuery().getDeleteSql();

        try(
            Connection conn = tm.getConnection();
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
        final String sql = getSqlQuery().getInsertSql();

        try(
            Connection conn = tm.getConnection();
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
        final String sql = getSqlQuery().getUpdateSql();

        try(
            Connection conn = tm.getConnection();
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
        final String sql = getSqlQuery().getUpdateIdSql();

        try(
            Connection conn = tm.getConnection();
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