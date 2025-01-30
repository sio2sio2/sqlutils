package edu.acceso.sqlutils.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    protected final DataSource ds;
    protected List<Class<? extends Crud<? extends Entity>>> daoClasses = new ArrayList<>();

    @SuppressWarnings("unchecked")
    public DaoConnection(Map<String, Object> options, Class<?>[] daoClasses) throws DataAccessException {
        ds = getDataSource(options);
        for(var daoClass: daoClasses) {
            registerDaoClass((Class<? extends Crud<? extends Entity>>) daoClass);
        }
    }

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

    /**
     * Obtiene de la caché el DataSource apropiado o lo crea y lo almacena en la caché
     * si no existía previamente.
     * @param opciones Las opciones con que se crear el DataSource.
     * @return El DataSource requerido.
     */
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
    public Connection getConnection() throws DataAccessException {
        try {
            return ds.getConnection();
        }
        catch(SQLException err) {
            throw new DataAccessException(err);
        }
    }

    /**
     * Registra una clase DAO que srive para hacer persistente una clase de objetos.
     * @param daoClass Las clase DAO a registrar.
     */
    public void registerDaoClass(Class<? extends Crud<? extends Entity>> daoClass) {
        daoClasses.add(daoClass);
    }

    /**
     * Obtiene en forma de array todas las clases DAO registradas.
     * @return
     */
    @SuppressWarnings("unchecked")
    public Class<? extends Crud<? extends Entity>>[] getDaoClasses() {
        return daoClasses.toArray(Class[]::new);
    }

    /**
     * Devuelve un objeto Dao que permite realizar las operaciones CRUD
     * @return El objeto Dao
     */
    public Dao getDao() {
        return new Dao(ds, getDaoClasses());
    }

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
