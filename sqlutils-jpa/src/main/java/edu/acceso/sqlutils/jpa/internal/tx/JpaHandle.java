package edu.acceso.sqlutils.jpa.internal.tx;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.internal.tx.TransactionHandle;
import edu.acceso.sqlutils.jpa.internal.EntityManagerWrapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.PersistenceException;
import jakarta.persistence.RollbackException;

/**
 * Implementación de {@link TransactionHandle} para JPA.
 */
public class JpaHandle implements TransactionHandle<EntityManager> {

    /**
     * El EntityManager subyacente que se utilizará para las operaciones de la transacción.
     */
    private final EntityManager handle;
    /**
     * El EntityTransaction asociado al EntityManager.
     */
    private EntityTransaction tx;

    /**
     * Crea un nuevo {@link JpaHandle} con el EntityManager proporcionado.
     * @param em El EntityManager que se usará en las operaciones de la transacción.
     */
    public JpaHandle(EntityManager em) {
        this.handle = em;
    }

    /**
     * Realiza las operaciones necesarias para iniciar una transacción JPA,
     * básicamente crear el {@link EntityTransaction} asociado al {@link EntityManager}
     * y llamar a su método begin().
     */
    @Override
    public void begin() throws DataAccessException {
        if(tx != null && tx.isActive()) {
            throw new DataAccessException("Ya hay una transacción activa en este EntityManager");
        }
        tx = handle.getTransaction();
        tx.begin();
    }

    /**
     * Realiza las operaciones necesarias para confirmar una transacción JPA.
     * @throws DataAccessException Si ocurre un error al intentar hacer commit de la transacción.
     * o si no hay una transacción activa para hacer commit.
     */
    @Override
    public void commit() throws DataAccessException {
        if(tx == null) throw new DataAccessException("No se ha abierto transacción");
        try {
            tx.commit();
        } catch (RollbackException e) {
            throw new DataAccessException("Error al hacer commit. Se realiza rollback.", e);
        } catch(IllegalStateException e) {
            throw new DataAccessException("Error al hacer commit. La transacción no está activa.", e);
        }
    }

    /**
     * Realiza las operaciones necesarias para hacer rollback de una transacción JPA.
     * @throws DataAccessException Si ocurre un error al intentar hacer el rollback,
     *  o si no hay una transacción activa.
     */
    @Override
    public void rollback() throws DataAccessException {
        if(tx == null) throw new DataAccessException("No se ha abierto transacción");
        try {
            tx.rollback();
        } catch(PersistenceException e) {
            throw new DataAccessException("Error al hacer rollback de la transacción", e);
        } catch(IllegalStateException e) {
            throw new DataAccessException("Error al hacer rollback. La transacción no está activa.", e);
        }
    }

    /**
     * Cierra el EntityManager subyacente.
     * @throws DataAccessException Si ocurre un error al intentar cerrarlo.
     */
    @Override
    public void close() throws DataAccessException {
        try {
            if(handle != null && handle.isOpen()) handle.close();
        } catch(IllegalStateException e) {
            throw new DataAccessException("Error al cerrar el EntityManager", e);
        }
    }

    /**
     * Verifica si el EntityManager subyacente está abierto.
     * @return {@code true} si el EntityManager está abierto, {@code false} si está cerrado o es null
     */
    @Override
    public boolean isOpen() {
        return handle != null && handle.isOpen();
    }

    /**
     * Devuelve el EntityManager subyacente que se está utilizando para las operaciones de la transacción. Este
     * EntityManager está protegido para evitar que el usuario pueda cerrarlo manualmente o intentar crear
     * una transacción manualmente sobre él.
     * @return El EntityManager solicitado
     */
    @Override
    public EntityManager getHandle() {
        return EntityManagerWrapper.createProxy(handle);
    }
}
