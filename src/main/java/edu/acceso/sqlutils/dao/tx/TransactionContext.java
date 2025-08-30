package edu.acceso.sqlutils.dao.tx;

import java.sql.Connection;
import java.util.Map;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.crud.AbstractCrud;
import edu.acceso.sqlutils.dao.crud.DaoProvider;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.RelationLoader;

/**
 * Contexto de transacción.
 * Proporciona un contexto para realizar operaciones de acceso a datos dentro de una transacción.
 * Permite obtener DAOs para diferentes entidades usando una misma conexión.
 */
public class TransactionContext {
    /** Conexión común para todas las operaciones. */
    private final Connection conn;
    private final DaoProvider daoProvider;
    private final Class<? extends RelationLoader> loaderClass;
    private final Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers;

    /**
     * Constructor del contexto de transacción.
     * @param conn Conexión a la base de datos
     * @param daoProvider Proveedor de DAOs (incluye las operaciones CRUD y la definición de las sentencias SQL).
     * @param loaderClass Clase que implementa el cargador de relaciones
     * @param mappers Mapa de mappers de entidades
     */
    public TransactionContext(Connection conn, DaoProvider daoProvider, Class<? extends RelationLoader> loaderClass, Map<Class<? extends Entity>, EntityMapper<? extends Entity>> mappers) {
        this.conn = conn;
        this.daoProvider = daoProvider;
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
            return (AbstractCrud<T>) daoProvider.getCrudClass().getConstructor(Connection.class, Class.class, Map.class, Class.class, Class.class)
                    .newInstance(conn, entityClass, mappers, daoProvider.getSqlQueryClass(), loaderClass);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(String.format("Error al crear la instancia de %s", daoProvider.getCrudClass().getSimpleName()), e);
        }
    }

}
