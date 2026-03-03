package edu.acceso.sqlutils.orm.simple.crud;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.orm.AbstractCrud;
import edu.acceso.sqlutils.orm.DaoFactory.DaoData;
import edu.acceso.sqlutils.orm.mapper.SqlTypesTranslator;
import edu.acceso.sqlutils.orm.minimal.Entity;
import edu.acceso.sqlutils.orm.relations.RelationLoader;
import edu.acceso.sqlutils.orm.simple.query.SimpleSqlQuery;
import edu.acceso.sqlutils.orm.tx.CacheManager;

/** 
 * Clase que implementa las operaciones CRUD para una entidad genérica T.
 * @param <T> Tipo de entidad que extiende {@link Entity}.
 */
public class SimpleCrud<T extends Entity> extends AbstractCrud<T> implements SimpleCrudInterface<T> {
    /** Logger para registrar información y errores. */
    private static final Logger logger = LoggerFactory.getLogger(SimpleCrud.class);

    /**
     * Constructor que recibe una clave identificativa y una clase que implementa {@link SimpleSqlQuery}.
     * @param key La clave que identifica al DaoFactory al que pertenece este DAO.
     * @param entityClass Clase de la entidad que maneja el DAO.
     */
    public SimpleCrud(String key, Class<T> entityClass) {
        super(key, entityClass);
        if(!(sqlQuery instanceof SimpleSqlQuery)) {
            throw new IllegalArgumentException("El sqlQuery debe ser una instancia de SimpleSqlQuery");
        }
    }

    /**
     * Constructor que crea una nueva instancia de {@link SimpleCrud} a partir de otro {@link SimpleCrud}
     * y con unos datos característicos nuevos.
     * @param original El DAO que se toma como base.
     * @param newData Los nuevos datos que caracterizan al nuevo DAO.
     */
    protected SimpleCrud(SimpleCrud<T> original, DaoData newData) {
        super(original, newData);
    }

    /**
     * Crea un nuevo DAO con el mismo DaoData pero con un nuevo FetchPlan.
     * @param newData Los nuevos datos que caracterizan al nuevo DAO.
     * @return Un nuevo DAO con los nuevos datos.
     */
    @Override
    public SimpleCrud<T> with(DaoData newData) {
        return new SimpleCrud<>(this, newData);
    }

    /**
     * Constructor que crea una nueva instancia de {@link SimpleCrud} a partir de un objeto {@link RelationLoader}.
     * 
     * <p>
     * Al crear el DAO a partir de un {@link RelationLoader}, se puede saber qué entidades
     * se han cargado previamente en la cadena de relaciones y evitar ciclos de referencia.
     * </p>
     * @param rl {@link RelationLoader} que origina este DAO.
     */
    public SimpleCrud(RelationLoader<T> rl) {
        super(rl);
    }

    /** 
     * Método auxiliar para obtener las consultas SQL que utiliza este DAO.
     * @return El {@link SimpleSqlQuery} asociado a este DAO.
     */
    private SimpleSqlQuery getSqlQuery() {
        return (SimpleSqlQuery) sqlQuery;
    }

    @Override
    public Optional<T> get(Long id) throws DataAccessException {
        final String sql = getSqlQuery().getSelectIdSql();

        CacheManager cm = data.cacheManager();

        T entity = cm.get(getEntityClass(), id);
        if (entity != null) return Optional.of(entity);

        try(
            Connection conn = data.tm().getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            return rs.next()
                ? Optional.of(cm.putInCache(mapper.resultSetToEntity(rs, this)))
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
            Connection conn = data.tm().getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            Stream<T> stream = SqlUtils.resultSetToStream(conn, pstmt, rs, fila -> mapper.resultSetToEntity(fila, this));
            logger.debug("Se ha obtenido un stream de registros de la tabla {}", mapper.getTableInfo().tableName());
            return stream.peek(data.cacheManager()::putInCache);
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
            Connection conn = data.tm().getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);

            pstmt.setObject(1, value, translator.getType());
            ResultSet rs = pstmt.executeQuery();

            Stream<T> stream = SqlUtils.resultSetToStream(conn, pstmt, rs, fila -> mapper.resultSetToEntity(fila, this));
            logger.debug("Se ha obtenido un stream de registros de la tabla {}", mapper.getTableInfo().tableName());
            return stream.peek(data.cacheManager()::putInCache);
        } catch (SQLException e) {
            logger.warn("Error al obtener los registros de la tabla {} donde {} = {}", mapper.getTableInfo().tableName(), column, value, e);
            throw new DataAccessException("Error al obtener los registros de la tabla '%s' donde %s = %d".formatted(mapper.getTableInfo().tableName(), column, value), e);
        }
    }

    @Override
    public boolean delete(Long id) throws DataAccessException {
        final String sql = getSqlQuery().getDeleteSql();

        try(
            Connection conn = data.tm().getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            pstmt.setLong(1, id);
            boolean result = pstmt.executeUpdate() > 0;
            if(result) {
                data.loggingManager().sendMessage(
                    getClass(),
                    Level.DEBUG,
                    "Eliminado registro ID=%s de la tabla %s".formatted(id, mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la eliminación del registro ID=%s de la tabla %s".formatted(id, mapper.getTableInfo().tableName())
                );
                data.cacheManager().deleteFromCache(getEntityClass(), id);
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
            Connection conn = data.tm().getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            mapper.EntityToParams(pstmt, entity);
            if(pstmt.executeUpdate()> 0) {
                data.loggingManager().sendMessage(
                    getClass(),
                    Level.DEBUG,
                    "Agregado registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la agregación del registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName())
                );
                data.cacheManager().putInCache(entity);
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
            Connection conn = data.tm().getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            mapper.EntityToParams(pstmt, entity);
            boolean result = pstmt.executeUpdate() > 0;
            if(result) {
                data.loggingManager().sendMessage(
                    getClass(),
                    Level.DEBUG,
                    "Actualizado registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la actualización del registro ID=%s en la tabla %s".formatted(entity.getId(), mapper.getTableInfo().tableName())
                );
                data.cacheManager().putInCache(entity);
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
            Connection conn = data.tm().getConnection();
            PreparedStatement pstmt = conn.prepareStatement(sql);
        ) {
            pstmt.setLong(1, newId);
            pstmt.setLong(2, oldId);
            boolean result = pstmt.executeUpdate() > 0;
            if(result) {
                data.loggingManager().sendMessage(
                    getClass(),
                    Level.DEBUG,
                    "Actualizado registro con ID=%s a ID=%s en la tabla %s".formatted(oldId, newId, mapper.getTableInfo().tableName()),
                    "Transacción fallida impide la actualización del registro con ID=%s a ID=%s en la tabla %s".formatted(oldId, newId, mapper.getTableInfo().tableName())
                );
                CacheManager cm = data.cacheManager();
                T entity = cm.deleteFromCache(getEntityClass(), oldId);
                if(entity != null) {
                    entity.setId(newId);
                    cm.putInCache(entity);
                }
            }
            return result;
        } catch (SQLException e) {
            logger.warn("Error al actualizar el ID del registro de {} a {} en la tabla {}", oldId, newId, mapper.getTableInfo().tableName(), e);
            throw new DataAccessException("Error al actualizar el ID del registro de %d a %d".formatted(oldId, newId), e);
        }
    }
}