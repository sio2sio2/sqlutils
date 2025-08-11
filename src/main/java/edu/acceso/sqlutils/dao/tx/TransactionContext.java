package edu.acceso.sqlutils.dao.tx;

import java.sql.Connection;
import java.util.Map;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.AbstractCrud;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.query.SqlQuery;

/**
 * Contexto de transacción.
 * Proporciona un contexto para realizar operaciones de acceso a datos dentro de una transacción.
 * Permite obtener DAOs para diferentes entidades usando una misma conexión.
 */
public class TransactionContext {
    /** Conexión común para todas las operaciones. */
    private final Connection conn;
    private final Class<? extends AbstractCrud<? extends Entity>> crudClass;
    private final Class<? extends SqlQuery> sqlQueryClass;
    private final Class<? extends RelationLoader> loaderClass;
    private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers;

    /**
     * Constructor del contexto de transacción.
     * 
     * @param conn Conexión a la base de datos
     * @param crudClass Clase que implementa las operaciones CRUD
     * @param sqlQueryClass Clase que implementa las sentencias SQL
     * @param loaderClass Clase que implementa el cargador de relaciones
     * @param mappers Mapa de mappers de entidades
     */
    public TransactionContext(Connection conn, Class<? extends AbstractCrud<? extends Entity>> crudClass, Class<? extends SqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass, Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers) {
        this.conn = conn;
        this.crudClass = crudClass;
        this.sqlQueryClass = sqlQueryClass;
        this.loaderClass = loaderClass;
        this.mappers = mappers;
    }

    /**
     * Obtiene un DAO para la clase de entidad especificada usando la conexión del contexto de transacción.
     * @param <T> Clase de la entidad
     * @param entityClass Clase de la entidad
     * @return DAO para la entidad especificada
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> AbstractCrud<T> getDao(Class<T> entityClass) {
        try {
            return (AbstractCrud<T>) crudClass.getConstructor(Connection.class, Class.class, Map.class, Class.class, Class.class)
                    .newInstance(conn, entityClass, mappers, sqlQueryClass, loaderClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(String.format("Error al crear la instancia de %s", crudClass.getSimpleName()), e);
        }
    }

}
