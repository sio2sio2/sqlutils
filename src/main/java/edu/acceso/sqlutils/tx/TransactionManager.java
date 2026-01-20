package edu.acceso.sqlutils.tx;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.tx.functional.Transactionable;
import edu.acceso.sqlutils.tx.functional.TransactionableR;

/**
 * Modela un gestor de transacciones que permite definir transacciones
 * mediante bloques try-with-resources:
 * <pre>
 *    try(
 *       Connection conn = DriverManager.getConnection(url);
 *       TransactionManager tm = new TransactionManager(conn);
 *    ) {
 *          // Operaciones incluidas en la misma transacción
 *          tm.commit(); // Forzosamente explícito.
 *    }
 *    catch(DataAccessError err) {
 *          // rollback implícito: no es necesario indicarlo.
 *          System.err.println(err.Message());
 *    }
 * </pre>
 */
public class TransactionManager implements AutoCloseable {

    private Connection conn;
    private boolean committed;
    private boolean originalAutoCommit;

    /**
     * Constructor de la clase.
     * @param conn Conexión con la que se harán todas las operaciones
     *      que formen parte de la transacción.
     * @throws SQLException Cuando se produce un error al manipular la conexión.
     */
    public TransactionManager(Connection conn) throws SQLException {
        setConn(conn);
        originalAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
    }

    /**
     * Devuelve la conexión sobre la que se hace la transacción.
     * @return El objeto que representa la conexión.
     */
    public Connection getConn() {
        return conn;
    }

    private void setConn(Connection conn) throws SQLException {
        if(conn == null) throw new IllegalArgumentException("La conexión no puede ser nula");
        if(!conn.isValid(0)) throw new SQLException("La conexión debe ser válida");
        this.conn = conn;
    }

    /**
     * Confirma las operaciones que se han hecho en la transacción.
     * @throws SQLException Cuando se produce un error al realizar la confirmación.
     */
    public void commit() throws SQLException {
        conn.commit();
        committed = true;
    }

    /**
     * Determina si la transacción forma parte de otra transacción.
     * Debe indicarse explícitamente, porque de lo contrario, se desecharán
     * automáticamente los cambios al cerrar la transacción.
     * @return true, si así es.
     */
    public boolean isNested() {
        return !originalAutoCommit;
    }

    /**
     * Cierra la transacción y desecha las operaciones pendientes.
     * @throws SQLException Si se produce algún error al desechar las operaciones.
     */
    @Override
    public void close() throws SQLException {
        rollback();
    }

    /**
     * Realiza un rollback manual.
     * @throws SQLException Si se produce algún error al realizar el rollback.
     */
    public void rollback() throws SQLException {
        if(!committed && !isNested()) conn.rollback();
        conn.setAutoCommit(originalAutoCommit);
    }

    /**
     * Permite definir una transacción mediante un función lambda que devuelve un resultado.
     * Tanto la confirmación como el desecho de las operaciones es implícito.
     * <pre>
     *     try (Connection conn = DriveManager.getConnection(conn)) {
     *         boolean result = transactionSQL(conn, c -> {
     *             // Operaciones que usan la conexión "c".
     *             return true; // o false, según convenga
     *         });
     *     }
     *     catch(DataAccessException err) {
     *          System.err.println(err.Message());
     *     }
     * </pre>
     * @param <T> El tipo de dato que devuelve la operación.
     * @param conn La conexión a la que pertenece la conexión
     * @param operations La función lambda que define las operaciones de la transacción.
     * @return El resultado de la operación.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public static <T> T transactionSQL(Connection conn, TransactionableR<T> operations) throws DataAccessException {
        boolean originalAutoCommit = true;
        boolean rollback = false;

        try {
            if(conn == null) throw new IllegalArgumentException("La conexión no puede ser nula");
            if(!conn.isValid(0)) throw new SQLException("La conexión debe ser válida");
            originalAutoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            
            return operations.run(conn);
        }
        catch(DataAccessException | RuntimeException | SQLException err) {
            try {
                rollback = true;
                conn.rollback();
                throw err;
            }
            catch(SQLException e) {
                throw new DataAccessException(e);
            }
        }
        finally {
            try {
                if(originalAutoCommit && !rollback) conn.commit();
            }
            catch(SQLException err) {
                throw new DataAccessException(err);
            }
        }
    }

    /**
     * Como {@link #transactionSQL(Connection, TransactionableR)}, pero genera la transacción
     * a partir de un {@link DataSource}.
     * @param <T> El tipo de dato que devuelve la operación.
     * @param ds El origen de datos desde el que se obtiene la conexión.
     * @param operations La función lambda que define las operaciones de la transacción.
     * @return El resultado de la operación.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public static <T> T transactionSQL(DataSource ds, TransactionableR<T> operations) throws DataAccessException {
        try (Connection conn = ds.getConnection()) {
            return transactionSQL(conn, operations);
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    /**
     * Como {@link #transactionSQL(Connection, TransactionableR)}, pero sin resultado.
     * <pre>
     *     try (Connection conn = DriveManager.getConnection(conn)) {
     *         transactionSQL(conn, c -> {
     *             // Operaciones que usan la conexión "c".
     *         });
     *     }
     *     catch(DataAccessException err) {
     *          System.err.println(err.getMessage());
     *     }
     * </pre>
     * @param conn La conexión a la que pertenece la conexión
     * @param operations La función lambda que define las operaciones de la transacción.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public static void transactionSQL(Connection conn, Transactionable operations) throws DataAccessException {
        transactionSQL(conn , c -> {
            operations.run(c);
            return null;
        });
    }

    /**
     * Como {@link #transactionSQL(Connection, Transactionable)}, pero genera la transacción
     * a partir de un {@link DataSource}.
     * @param ds El origen de datos desde el que se obtiene la conexión.
     * @param operations La función lambda que define las operaciones de la transacción.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public static void transactionSQL(DataSource ds, Transactionable operations) throws DataAccessException {
        try (Connection conn = ds.getConnection()) {
            transactionSQL(conn, operations);
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }
}