package edu.acceso.sqlutils.jdbc.internal.tx;

import java.sql.Connection;
import java.sql.SQLException;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.internal.tx.TransactionHandle;
import edu.acceso.sqlutils.jdbc.internal.ConnectionWrapper;

/**
 * Particularización de {@link TransactionHandle} para JDBC.
 */
public class JdbcHandle implements TransactionHandle<Connection> {

    /**
     * La conexión JDBC subyacente que se utilizará para las operaciones de la transacción.
     */
    private final Connection handle;
    /**
     * El estado original de auto-commit de la conexión, para restaurarlo después de la transacción.
     */
    private final boolean originalAutoCommit;

    /**
     * Crea un nuevo {@link JdbcHandle} con la conexión JDBC proporcionada.
     * @param conn La conexión JDBC que se usará en las operaciones de la transacción.
     */
    public JdbcHandle(Connection conn) {
        this.handle = conn;
        try {
            this.originalAutoCommit = conn.getAutoCommit();
        } catch (SQLException e) {
            throw new DataAccessException("Error al obtener el estado de auto-commit de la conexión", e);
        }
    }

    /**
     * Realiza las operaciones necesarias para iniciar una transacción con
     * el objeto {@link Connection} de una transacción JDBC.
     * @throws DataAccessException Si ocurre un error al intentar desactivar el auto-commit de la conexión.
     */
    @Override
    public void begin() throws DataAccessException {
        try {
            handle.setAutoCommit(false);
        } catch (SQLException e) {
            throw new DataAccessException("Error al iniciar la transacción", e);
        }
    }

    /**
     * Realiza las operaciones necesarias para confirmar una transacción con el objeto {@link Connection} de una transacción JDBC.
     * @throws DataAccessException Si ocurre un error al intentar hacer commit de la transacción
     */
    @Override
    public void commit() throws DataAccessException {
        SQLException commitException = null;

        try {
            handle.commit();
        } catch (SQLException e) {
            commitException = e;
            throw new DataAccessException("Error al hacer commit de la transacción", e);
        } finally {
            try {
                handle.setAutoCommit(originalAutoCommit);
            } catch (SQLException e) {
                if (commitException != null) {
                    commitException.addSuppressed(e);
                } else {
                    throw new DataAccessException("Error al cerrar la conexión tras commit", e);
                }
            }
        }
    }

    /**
     * Realiza las operaciones necesarias para deshacer una transacción con el objeto {@link Connection} de una transacción JDBC.
     * @throws DataAccessException Si ocurre un error al intentar hacer rollback de la transacción
     */
    @Override
    public void rollback() throws DataAccessException {
        SQLException rollbackException = null;

        try {
            handle.rollback();
        } catch (SQLException e) {
            rollbackException = e;
            throw new DataAccessException("Error al hacer rollback de la transacción", e);
        } finally {
            try {
                handle.setAutoCommit(originalAutoCommit);
            } catch (SQLException e) {
                if (rollbackException != null) {
                    rollbackException.addSuppressed(e);
                } else {
                    throw new DataAccessException("Error al cerrar la conexión tras rollback", e);
                }
            }
        }
    }

    /**
     * Cierra la conexión JDBC subyacente.
     * @throws DataAccessException Si ocurre un error al intentar cerrar la conexión.
     */
    @Override
    public void close() throws DataAccessException {
        try {
            handle.close();
        } catch (SQLException e) {
            throw new DataAccessException("Error al cerrar la conexión", e);
        }
    }

    /**
     * Verifica si la conexión JDBC subyacente está abierta.
     * @return {@code true} si la conexión está abierta, {@code false} en caso contrario.
     * @throws DataAccessException Si ocurre un error al intentar verificar el estado de la conexión.
     */
    @Override
    public boolean isOpen() {
        try {
            return !handle.isClosed();
        } catch (SQLException e) {
            throw new DataAccessException("Error al verificar si la conexión está abierta", e);
        }
    }

    /**
     * Devuelve la conexión JDBC subyacente. Esta conexión está protegida para evitar que el programador la cierre directamente,
     * lo que podría causar problemas en la gestión de la transacción.
     * @return La conexión solicitada.
     */
    @Override
    public Connection getHandle() {
        return ConnectionWrapper.createProxy(handle);
    }
}
