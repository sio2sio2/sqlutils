package edu.acceso.sqlutils.persistence.jpa;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.Config;
import edu.acceso.sqlutils.DbmsSelector;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.jpa.JpaConnection;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Titularidad;
import edu.acceso.sqlutils.tx.Transactionable;
import edu.acceso.sqlutils.tx.TransactionableR;
import edu.acceso.sqlutils.tx.event.LoggingManager;
import jakarta.persistence.EntityManager;

/**
 * Clase de conexión a la base de datos utilizando JPA.
 * Aplica un patrón de diseño Singleton para garantizar que solo exista una instancia de Conexion.
 */
public class Conexion {
    private static Logger logger = LoggerFactory.getLogger(Conexion.class);

    private static final String DB_KEY = "DB";

    private static Conexion instance;
    private final JpaConnection jc;

    private Conexion() {
        Config config = Config.get();

        String url = config.getDbUrl();
        String user = config.getUser();
        String password = config.getPassword();

        DbmsSelector dbms = DbmsSelector.fromUrl(url);
        JpaProvider jpaProvider = JpaProvider.HIBERNATE
            .withSgbd(dbms);

        Map<String, Object> props = new HashMap<>();
        props.put("jakarta.persistence.jdbc.driver", dbms.getDriver());
        props.put("jakarta.persistence.jdbc.url", url);
        props.put("jakarta.persistence.jdbc.user", user);
        props.put("jakarta.persistence.jdbc.password", password);
        props.put(jpaProvider.getDialectKey(), jpaProvider.getDialect());

        // No definimos el DataSource, pero como se ha cargado el módulo sqlutils-hikaricp,
        // el DataSource se construirá con la librería HikariCP.
        // Si no se hubiera cargado, se usaría el mecanismo interno del proveedor de JPA.

        jc = JpaConnection.create(DB_KEY, props)
            .withTransactionManager(Map.of(LoggingManager.KEY, new LoggingManager()));

    }

    /**
     * Crea una instancia de Conexion si aún no existe y la devuelve.
     * @return Una instancia de Conexion.
     * @throws IllegalStateException Si ya existe una instancia de Conexion.
     */
    public static Conexion create() {
        if(instance != null) throw new IllegalStateException("La instancia de Conexion ya ha sido creada.");

        instance = new Conexion();
        logger.debug("Instancia de Conexion creada.");

        return instance.inicializar();
    }

    /**
     * Devuelve la instancia existente de Conexion.
     * @return La instancia existente de Conexion.
     * @throws IllegalStateException Si no existe una instancia de Conexion.
     */
    public static Conexion get() {
        if(instance == null) throw new IllegalStateException("La instancia de Conexion no ha sido creada. Llama a create() primero.");
        return instance;
    }

    private Conexion inicializar() {
        transaction(ctxt -> {
            EntityManager em = ctxt.handle();

            Centro c = em.find(Centro.class, 11004866L);
            if(c != null) {
                logger.debug("La base de datos ya está inicializada");
                return;
            }

            Centro[] centros = new Centro[] {
                new Centro(11004866L, "IES Castillo de Luna", Titularidad.PUBLICA),
                new Centro(11700603L, "IES Pintor Juan Lara", Titularidad.PUBLICA),
                new Centro(11007533L, "IES Arroyo Hondo", Titularidad.PUBLICA)
            };

            for(Centro centro : centros) {
                em.persist(centro);
            }
            logger.debug("Poblada la base de datos con los {} centros iniciales.", centros.length);
        });
        return this;
    }

    /**
     * Ejecuta un bloque de código dentro de una transacción, devolviendo un resultado.
     * @param <T> Tipo del resultado esperado.
     * @param actions Bloque de código a ejecutar dentro de la transacción.
     * @return El resultado devuelto por el bloque de código.
     * @throws DataAccessException Si hay un error de acceso a datos durante la transacción.
     */
    public <T> T transactionR(TransactionableR<EntityManager, T> actions) throws DataAccessException {
        return jc.getTransactionManager().transaction(actions);
    }

    /**
     * Ejecuta un bloque de código dentro de una transacción.
     * @param actions Bloque de código a ejecutar dentro de la transacción.
     * @throws DataAccessException Si hay un error de acceso a datos durante la transacción.
     */
    public void transaction(Transactionable<EntityManager> actions) throws DataAccessException {
        jc.getTransactionManager().transaction(actions);
    }
}
