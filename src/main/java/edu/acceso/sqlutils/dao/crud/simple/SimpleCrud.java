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
import org.slf4j.event.Level;

import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.Cache;
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
                      Class<? extends SimpleSqlQuery> sqlQueryClass, Class<? extends RelationLoader<? extends Entity>> loaderClass) {
        super(key, entityClass, mappers, sqlQueryClass, loaderClass);
    }

    /**
     * Constructor que crea una nueva instancia de {@link SimpleCrud} a partir de un objeto {@link RelationLoader}.
     * 
     * <p>
     * Este objeto {@link SimpleCrud} se construye compartiendo los mismos parámeros que el DAO original
     * que creó el {@link RelationLoader} que se le pasa como argumento. Esto permite conocer
     * cuál es el historial de entidades cargadas y evitar ciclos de referencia.
     * </p>
     * @param originalDao DAO original del que se crea este nuevo DAO.
     * @param rl {@link RelationLoader} que origina este DAO.
     */
    public SimpleCrud(SimpleCrud<? extends Entity> originalDao, RelationLoader<T> rl) {
        super(originalDao, rl);
    }

    private SimpleSqlQuery getSqlQuery() {
        return (SimpleSqlQuery) sqlQuery;
    }

    /**
     * Busca la entidad en la caché de la transacción actual.
     * @param id ID de la entidad a buscar.
     * @return Entidad encontrada en la caché o {@code null} si no existe.
     */
    protected T searchInCache(Long id) {
        Cache cache = getCache();
        if (cache == null) return null;

        T entity = (T) cache.get(getEntityClass(), id);
        return entity;
    }

    @Override
    public Optional<T> get(Long id) throws DataAccessException {
        final String sql = sqlQuery.getSelectIdSql();
        checkTransactionActive();

        T entity = searchInCache(id);
        if (entity != null) return Optional.of(entity);

        try(
            Connection conn = tm.getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            return rs.next()
                ? Optional.of(putInCache(mapper.resultSetToEntity(rs, this)))
                : Optional.empty();
        } catch (SQLException e) {
            logger.warn("Error al obtener el registro con ID {} de la tabla {}", id, mapper.getTableInfo().tableName(), e);
            throw new DataAccessException("Error al obtener el registro con ID %d".formatted(id), e);
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

            Stream<T> stream = SqlUtils.resultSetToStream(conn, pstmt, rs, fila -> mapper.resultSetToEntity(fila, this));
            logger.debug("Se ha obtenido un stream de registros de la tabla {}", mapper.getTableInfo().tableName());
            return stream.peek(this::putInCache);
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

            Stream<T> stream = SqlUtils.resultSetToStream(conn, pstmt, rs, fila -> mapper.resultSetToEntity(fila, this));
            logger.debug("Se ha obtenido un stream de registros de la tabla {}", mapper.getTableInfo().tableName());
            return stream.peek(this::putInCache);
        } catch (SQLException e) {
            logger.warn("Error al obtener los registros de la tabla {} donde {} = {}", mapper.getTableInfo().tableName(), column, value, e);
            throw new DataAccessException("Error al obtener los registros de la tabla '%s' donde %s = %d".formatted(mapper.getTableInfo().tableName(), column, value), e);
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
            boolean result = pstmt.executeUpdate() > 0;
            if(result) {
                tm.deferLog(getClass(),
                    Level.DEBUG,
                    "Eliminado registro ID=%s de la tabla %s".formatted(id, mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la eliminación del registro ID=%s de la tabla %s".formatted(id, mapper.getTableInfo().tableName())
                );
                deleteFromCache(id);
            }
            return result;
        } catch (SQLException e) {
            logger.warn("Error al eliminar el registro con ID {} de la tabla {}", id, mapper.getTableInfo().tableName(), e);
            throw new DataAccessException("Error al eliminar el registro con ID %d".formatted(id), e);
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
            if(pstmt.executeUpdate()> 0) {
                tm.deferLog(getClass(),
                    Level.DEBUG,
                    "Agregado registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la agregación del registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName())
                );
                putInCache(entity);
            }
        } catch (SQLException e) {
            logger.warn("Error al insertar el registro con ID {} de la tabla {}", entity.getId(), mapper.getTableInfo().tableName(), e);
            throw new DataAccessException("Error al insertar el registro con ID %d".formatted(entity.getId()), e);
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
            boolean result = pstmt.executeUpdate() > 0;
            if(result) {
                tm.deferLog(getClass(),
                    Level.DEBUG,
                    "Actualizado registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la actualización del registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName())
                );
                putInCache(entity);
            }
            return result;
        } catch (SQLException e) {
            logger.warn("Error al actualizar el registro con ID {} de la tabla {}", entity.getId(), mapper.getTableInfo().tableName(), e);
            throw new DataAccessException("Error al actualizar el registro con ID %d".formatted(entity.getId()), e);
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
            boolean result = pstmt.executeUpdate() > 0;
            if(result) {
                tm.deferLog(getClass(),
                    Level.DEBUG,
                    "Actualizado registro con ID=%s a ID=%s en la tabla %s".formatted(oldId, newId, mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la actualización del registro con ID=%s a ID=%s en la tabla %s".formatted(oldId, newId, mapper.getTableInfo().tableName())
                );
                T entity = deleteFromCache(oldId);
                if(entity != null) {
                    entity.setId(newId);
                    putInCache(entity);
                }
            }
            return result;
        } catch (SQLException e) {
            logger.warn("Error al actualizar el ID del registro de {} a {} en la tabla {}", oldId, newId, mapper.getTableInfo().tableName(), e);
            throw new DataAccessException("Error al actualizar el ID del registro de %d a %d".formatted(oldId, newId), e);
        }
    }
}