package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import edu.acceso.sqlutils.Entity;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.transaction.TransactionManager;

public abstract class DaoConnection {

    @FunctionalInterface
    public interface DaoTransactionable {
        void run(Dao dao) throws DataAccessException;
    }

    private static Map<String, DataSource> cache = new HashMap<>();

    /**
     * Genera una clave para identificar el DataSource y distinguirlo de otros.
     * @param opciones Las opciones de conexión.
     * @return La clave generada.
     */
    protected abstract String generateKey(Map<String, Object> opciones);
    /**
     * Implementa la creación de un nuevo DataSource.
     * @param opciones Las opciones de conexión.
     * @return El objeto creado.
     */
    protected abstract DataSource createDataSource(Map<String, Object> opciones);

    protected DataSource getDataSource(Map<String, Object> opciones) {
        String key = generateKey(opciones);
        if(cache.containsKey(key)) return cache.get(key);

        DataSource ds = createDataSource(opciones);
        cache.put(key, ds);
        return ds;
    }

    /**
     * Devuelve una conexión a la base de datos.
     * @return El objeto de conexión.
     */
    public abstract Connection getConnection() throws DataAccessException;

    /**
     * Devuelve la lista de clases DAO definidas para la base de datos.
     * @return La lista.
     */
    public abstract Class<? extends Crud<? extends Entity>>[] getDaoClasses();

    /**
     * Devuelve un objeto DAO que permite realizar las operaciones CRUD
     * @return El objeto DAO
     */
    public abstract Dao getDao();

    /**
     * Permite aglutinar operaciones CRUD dentro de una misma transacción.
     * @param operaciones Todas las operaciones que se desea integrar
     *   dentro de la transacción.
     */
    public void transaction(DaoTransactionable operaciones) throws DataAccessException {
        try(Connection conn = getConnection()) {
            TransactionManager.transactionSQL(conn, c -> {
                operaciones.run(new Dao(c, getDaoClasses()));
            });
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }
}
