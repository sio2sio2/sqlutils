package edu.acceso.sqlutils.backend;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.ArchivoWrapper;
import edu.acceso.sqlutils.Config;
import edu.acceso.sqlutils.DbmsSelector;
import edu.acceso.sqlutils.backend.mappers.CentroMapper;
import edu.acceso.sqlutils.backend.mappers.EstudianteMapper;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.jdbc.SqlUtils;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.orm.DaoFactory;
import edu.acceso.sqlutils.orm.DaoFactory.DaoData;
import edu.acceso.sqlutils.orm.SqlQueryFactory;
import edu.acceso.sqlutils.orm.minimal.Entity;
import edu.acceso.sqlutils.orm.relations.FetchPlan;
import edu.acceso.sqlutils.orm.simple.crud.SimpleListCrud;
import edu.acceso.sqlutils.orm.simple.query.SimpleSqlQuery;
import edu.acceso.sqlutils.orm.simple.query.SimpleSqlQueryGeneric;


/**
 * Clase principal del backend que inicializa la base de datos y
 * proporciona acceso a la fábrica de DAOs.
 */
public class Conexion {
    private static Logger logger = LoggerFactory.getLogger(Conexion.class);

    private static final String DB_KEY = "DB";

    /** Instancia única de la clase Conexion (patrón Singleton) */
    private static Conexion instance;

    /** Fábrica de DAOs para acceder a las entidades del backend */
    private final DaoFactory daoFactory;

    /** Interfaz funcional para ejecutar transacciones con DAOs de Centro y Estudiante */
    @FunctionalInterface
    public static interface TransactionInterface {
        public void run() throws DataAccessException;
    }

    /** Interfaz funcional para ejecutar transacciones que devuelven resultado con DAOs de Centro y Estudiante */
    @FunctionalInterface
    public static interface TransactionInterfaceR<T> {
        public T run() throws DataAccessException;
    }

    /**
     * Constructor privado para inicializar el backend con un pool de conexiones y una fábrica de DAOs.
     * @Param key Clave que identifica la fuente de datos.
     * @param ds Pool de conexiones a la base de datos.
     * @param daoFactory Fábrica de DAOs para acceder a las entidades del backend.
     */
    private Conexion(DaoFactory daoFactory) {
        this.daoFactory = daoFactory;
    }

    /**
     * Método estático para crear una instancia de Backend y inicializar la base de datos.
     * @return Una instancia de Backend con la base de datos inicializada.
     * @throws DataAccessException Si ocurre un error al acceder a los datos o al inicializar la base de datos.
     */
    public static Conexion create() throws IOException, DataAccessException {
        if(instance != null) throw new IllegalStateException("La conexión ya se inicializó");

        Config config = Config.getInstance();


        // No es necesario definir este constructor de fábrica de consultas SQL, porque es el genérico y
        // el DaoFactory se encarga de construirlo automáticamente, si no se le facilita uno.
        SqlQueryFactory.Builder<?> sqlQueryBuilder = SqlQueryFactory.Builder.create(SimpleSqlQuery.class)
                // No es necesario, porque se usa la implementación genérica,
                // pero se muestra como ejemplo de cómo se puede usar una distinta con otro SGBD.
                .register(DbmsSelector.SQLITE, SimpleSqlQueryGeneric.class)
                // Para todos los SGBD se usa la implementación genérica.
                .register("*", SimpleSqlQueryGeneric.class);


        // Se define la fábrica de objetos DAO a partir de los mappers de las entidades.
        // Para la obtención de relaciones se usa carga perezosa (Lazy Loading).
        // sqlQueryBuilder podría no pasarse.
        DaoFactory daoFactory = DaoFactory.Builder.create(DB_KEY, SimpleListCrud.class, sqlQueryBuilder)
            .with(FetchPlan.LAZY)
            .registerMapper(CentroMapper.class)
            .registerMapper(EstudianteMapper.class)
            .get(config.getDbUrl(), config.getUser(), config.getPassword());

        instance = new Conexion(daoFactory)
            .inicializar(config.getInput());

        return instance;
    }

    /**
     * Obtiene la fuente de datos para la conexión a la base de datos.
     * @return Fuente de datos para la conexión a la base de datos.
     */
    public static Conexion getInstance() {
        if(instance == null) throw new IllegalStateException("La conexión no se ha inicializado");
        return instance;
    }

    private Conexion inicializar(ArchivoWrapper guion) throws IOException, DataAccessException {
        try(InputStream st = guion.openStream()) {
            daoFactory.getTransactionManager().transaction(ctxt ->{
                Connection conn = ctxt.handle();
                if(!SqlUtils.isDatabaseEmpty(conn)) {
                    SqlUtils.executeSQL(conn, st);
                    logger.info("Base de datos inicializada correctamente.");
                }
            });
        }

        return this;
    }

    /**
     * Obtiene el DAO correspondiente a la clase indicada.
     * @param <T> Tipo de entidad.
     * @param clazz Clase de la entidad.
     * @return DAO correspondiente.
     */
    @SuppressWarnings("unchecked")
    public <T extends Entity> SimpleListCrud<T> getDao(Class<T> clazz) {
        return (SimpleListCrud<T>) switch(clazz.getSimpleName()) {
            case "Centro" -> daoFactory.getDao(Centro.class);
            case "Estudiante" -> daoFactory.getDao(Estudiante.class);
            default -> throw new IllegalArgumentException("No hay DAO para la clase " + clazz.getSimpleName());
        };
    }

    /**
     * Obtiene los datos de la fábrica de DAOs, para poder modificar
     * con comodidad el plan de carga de relaciones al vuelo.
     * @return Los datos solicitados.
     */
    public DaoData getDaoData() {
        return daoFactory.getDaoData();
    }

    /**
     * Ejecuta una transacción que devuelve resultados con múltiples DAO.
     * @param operations Código con las operaciones que constituyen la transacción.
     * @throws DataAccessException Si hay un error durante la transacción.
     */
    public <T> T transactionR(TransactionInterfaceR<T> operations) throws DataAccessException {
        //return daoFactory.getTransactionManager().transaction(conn -> { return operations.run(); });
        return daoFactory.getTransactionManager().transaction(conn -> { return operations.run(); });
    }

    /**
     * Ejecuta una transacción con múltiples DAO.
     * @param operations Código con las operaciones
     * @throws DataAccessException Si hay un error durante la transacción.
     */
    public void transaction(TransactionInterface operations) throws DataAccessException {
        daoFactory.getTransactionManager().transaction(conn -> { operations.run(); });
    }
}