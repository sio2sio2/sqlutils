package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Stream;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.ConnProvider;
import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.crud.Crud;
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
public class DaoCrud<T extends Entity> implements Crud<T> {
    /** Logger para registrar información y errores. */
    private static final Logger logger = LoggerFactory.getLogger(DaoCrud.class);

    /** DAO que proporciona acceso a datos relacionados. */
    private final ConnProvider cp;
    /** Clase que implementa las sentencias SQL para las operaciones CRUD. */
    private final SqlQuery sqlQuery;
    /**
     * Fábrica de DAOs con la que se generó este objeto y que sirve para obtener entidades relacionadas
     * (cuando la entidad tiene claves foráneas).
     */
    private final DaoFactory daoF;
    /**
     * Mapper de la entidad T que mapea registros de la base de datos a objetos de tipo T.
     * Este mapper es responsable de convertir filas de ResultSet en instancias de T y viceversa.
     */
    private final EntityMapper<T> mapper;
    /**
     * Cargador de relaciones que se utiliza para cargar entidades relacionadas.
     * Este cargador es responsable de manejar las relaciones entre entidades y cargar datos adicionales según sea necesario.
     */
    private final RelationLoader loader;

    /**
     * Constructor que recibe un {@link DataSource} y una clase que implementa {@link SqlQuery}.
     * @param ds Una fuente de datos para obtener conexiones a la base de datos.
     * @param mapper El EntityMapper que mapea entidades a registros de la base de
     * @param daoF La fábrica de DAOs que se utilizará para obtener entidades relacionadas.
     */
    @SuppressWarnings("unchecked")
    public DaoCrud(DataSource ds, Class<T> entityClass, DaoFactory daoF) {
        this.cp = new ConnProvider(ds);
        this.daoF = daoF;
        this.mapper = (EntityMapper<T>) daoF.mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.sqlQuery = createSqlQueryInstance(daoF.sqlQueryClass, mapper);
        this.loader = createRelationLoaderInstance(daoF.loaderClass);
    }

    /**
     * Constructor que recibe una {@link Connection} y una clase que implementa {@link SqlQuery}.
     * @param conn Una conexión a la base de datos.
     * @param mapper El EntityMapper que mapea entidades a registros de la base de
     * @param daoF La fábrica de DAOs que se utilizará para obtener entidades relacionadas.
     */
    @SuppressWarnings("unchecked")
    public DaoCrud(Connection conn, Class<T> entityClass, DaoFactory daoF) {
        this.cp = new ConnProvider(conn);
        this.mapper = (EntityMapper<T>) daoF.mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.daoF = daoF;
        this.sqlQuery = createSqlQueryInstance(daoF.sqlQueryClass, mapper);
        this.loader = createRelationLoaderInstance(daoF.loaderClass);
    }

    /**
     * Constructor que crea una nueva instancia de {@link DaoCrud} a partir de otro {@link DaoCrud}.
     * 
     * <p>
     * Un {@link DaoCrud} obtenido de este modo comparte el cargador de relaciones (véase
     * {@link RelationLoader}) con el original, lo que permite que éste conserve el historial
     * de todas las relaciones cargadas.
     * </p>
     * @param <E> Tipo de entidad del {@link DaoCrud} original.
     * @param dao El {@link DaoCrud} original a partir del cual se obtiene el nuevo.
     * @param entityClass La clase de la entidad que maneja el nuevo {@link DaoCrud}.
     */
    @SuppressWarnings("unchecked")
    public <E extends Entity> DaoCrud(DaoCrud<E> dao, Class<T> entityClass) {
        this.cp = dao.cp;
        this.mapper = (EntityMapper<T>) dao.daoF.mappers.get(entityClass);
        if (mapper == null) {
            throw new IllegalArgumentException(String.format("La entidad %s no está registrada", entityClass.getSimpleName()));
        }
        this.daoF = dao.daoF;
        this.sqlQuery = createSqlQueryInstance(daoF.sqlQueryClass, mapper);
        this.loader = dao.loader;
    }

    /**
     * Obtiene la fábrica de DAOs asociada a este {@link DaoCrud}.
     * @return La fábrica de DAOs.
     */
    DaoFactory getDaoFactory() {
        return daoF;
    }

    /**
     * Crea una instancia de RelationLoader utilizando la clase proporcionada.
     * @param loaderClass La clase que implementa RelationLoader.
     * @return Una instancia de RelationLoader.
     */
    private RelationLoader createRelationLoaderInstance(Class<? extends RelationLoader> loaderClass) {
        try {
            return loaderClass.getConstructor(DaoCrud.class)
                .newInstance(this);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error al crear instancia de RelationLoader", e);
        }
    }

    /**
     * Crea una instancia de SqlQuery utilizando la clase proporcionada.
     * @param sqlQueryClass La clase que implementa SqlQuery.
     * @return Una instancia de SqlQuery.
     */
    private static SqlQuery createSqlQueryInstance(Class<? extends SqlQuery> sqlQueryClass, EntityMapper<? extends Entity> mapper) {
        try {
            return sqlQueryClass.getConstructor(String.class, String.class, String[].class).newInstance(
                mapper.getTableInfo().tableName(),
                mapper.getTableInfo().idColumn().name(),
                mapper.getTableInfo().getColumnNames()
            );
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Error al crear instancia de SqlQuery", e);
        }
    }

    /**
     * Obtiene el traductor de tipos SQL para un campo y un valor dado.
     * El método permite saber cómo traducir a SQL el valor de un campo
     * al usar {@link #get(String, Object)}.
     * @param field El campo para el que se obtiene el traductor.
     * @param value El valor para el que se obtiene el traductor.
     * @return Un traductor de tipos SQL para el campo y el valor dados.
     */
    private SqlTypesTranslator getTranslator(String field, Object value) {
        Class<?> attrClass = mapper.getTableInfo().getFieldType(field);
        // No se incluyó la información del tipo en el mapper,
        // por lo que hay que averiguarlo por reflexión.
        if(attrClass == null) {
            Class<? extends Entity> entityClass = daoF.mappers.entrySet()
                .stream()
                .filter(e -> e.getValue().getClass().isAssignableFrom(mapper.getClass()))
                .map(e -> e.getKey())
                .findFirst().orElse(null);
            assert entityClass != null: "No se encontró la clase de entidad asociada al mapper";
            try {
                attrClass = entityClass.getDeclaredField(field).getType();
            } catch (NoSuchFieldException e) {
                throw new RuntimeException(String.format("El campo %s no existe en la entidad %s", field, entityClass.getSimpleName()), e);
            }
        }
        return (Entity.class.isAssignableFrom(attrClass))
            ? new SqlTypesTranslator(Long.class, ((Entity) value).getId())
            : new SqlTypesTranslator(attrClass, value);
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