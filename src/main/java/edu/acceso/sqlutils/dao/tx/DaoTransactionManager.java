package edu.acceso.sqlutils.dao.tx;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.tx.TransactionManager;

/**
 * Gestor de transacciones específico para los objetos DAO.
 * Añade una caché de entidades cargadas para optimizar el acceso a los datos.
 */
public class DaoTransactionManager extends TransactionManager {
    private static final Logger logger = LoggerFactory.getLogger(DaoTransactionManager.class);

    /** Caché que almacena entidades ya cargadas */
    private final ThreadLocal<Cache> cache = new ThreadLocal<>();

    /**
     * Constructor privado que permite implementar el patrón Multiton.
     * @param key Identificador de la instancia
     * @param ds Fuente de datos asociada
     */
    private DaoTransactionManager(String key, DataSource ds) {
        super(key, ds);
    }

    /**
     * Crea una nueva instancia de DaoTransactionManager.
     * @param key Identificador de la instancia.
     * @param ds Fuente de datos asociada.
     * @return La nueva instancia creada.
     * @throws IllegalArgumentException Si ya existe una instancia con la misma clave.
     */
    public static DaoTransactionManager create(String key, DataSource ds) {
        DaoTransactionManager tm = new DaoTransactionManager(key, ds);
        registerInstance(key, tm);
        return tm;
    }

    /**
     * Gancho que se ejecuta al iniciar la transacción: inicializa la caché de entidades.
     */
    protected void onBegin() {
        cache.set(new Cache());
        logger.trace("Creada nueva caché de entidades para la trasacción");
    }

    /**
     * Gancho que se ejecuta al cerrar la transacción: limpia la caché de entidades.
     */
    protected void onClose() {
        cache.remove();
        logger.trace("Eliminada la caché de entidades al cerrar la transacción");
    }

    /** Obtiene la caché de entidades de la transacción actual
     * @return Caché de entidades
     */
    public Cache getCache() {
        return cache.get();
    }
}