package edu.acceso.sqlutils.dao.tx;

import java.sql.Connection;

import edu.acceso.sqlutils.crud.Crud;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.DaoFactory;
import edu.acceso.sqlutils.dao.DaoCrud;

/**
 * Contexto de transacción.
 * Proporciona un contexto para realizar operaciones de acceso a datos dentro de una transacción.
 * Permite obtener DAOs para diferentes entidades usando una misma conexión.
 */
public class TransactionContext {
    /** Conexión común para todas las operaciones. */
    private final Connection conn;
    /** Fábrica de DAOs para crear DAOs específicos de entidades. */
    private final DaoFactory daoF;

    /**
     * Constructor del contexto de transacción.
     * 
     * @param conn Conexión a la base de datos
     * @param daoF Fábrica de DAOs para crear DAOs específicos de entidades
     */
    public TransactionContext(Connection conn, DaoFactory dao) {
        this.conn = conn;
        this.daoF = dao;
    }

    /**
     * Obtiene un DAO para la clase de entidad especificada usando la conexión del contexto de transacción.
     * @param <T> Clase de la entidad
     * @param entityClass Clase de la entidad
     * @return DAO para la entidad especificada
     */
    public <T extends Entity> Crud<T> getDao(Class<T> entityClass) {
        return new DaoCrud<>(conn, entityClass, daoF);
    }

}
