package edu.acceso.sqlutils.backend;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.ArchivoWrapper;
import edu.acceso.sqlutils.Config;
import edu.acceso.sqlutils.ConnectionPool;
import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.backend.mappers.CentroMapper;
import edu.acceso.sqlutils.backend.mappers.EstudianteMapper;
import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.DaoFactory;
import edu.acceso.sqlutils.dao.crud.DaoProvider;
import edu.acceso.sqlutils.dao.crud.SqlQueryFactory;
import edu.acceso.sqlutils.dao.crud.simple.SimpleListCrud;
import edu.acceso.sqlutils.dao.crud.simple.SimpleSqlQuery;
import edu.acceso.sqlutils.dao.crud.simple.SimpleSqlQueryGeneric;
import edu.acceso.sqlutils.dao.relations.LoaderFactory;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;
import edu.acceso.sqlutils.modelo.Estudiante;
import edu.acceso.sqlutils.tx.TransactionManager;


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
    /** Pool de conexiones a la base de datos */
    private final ConnectionPool cp;

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
    private Conexion(String key, ConnectionPool cp, DaoFactory daoFactory) {
        this.daoFactory = daoFactory;
        this.cp = cp;
    }

    /**
     * Método estático para crear una instancia de Backend y inicializar la base de datos.
     * @return Una instancia de Backend con la base de datos inicializada.
     * @throws DataAccessException Si ocurre un error al acceder a los datos o al inicializar la base de datos.
     */
    public static Conexion create() throws IOException, DataAccessException {
        if(instance != null) throw new IllegalStateException("La conexión ya se inicializó");

        Config config = Config.getInstance();
        ConnectionPool cp = ConnectionPool.create(DB_KEY, config.getDbUrl(), config.getUser(), config.getPassword());

        // Se configura cuáles son las sentencias SQL para las operaciones CRUD.
        SqlQueryFactory sqlQueryFactory = SqlQueryFactory.Builder.create("centros")
                // No es necesario, porque se usa la implementación genérica,
                // pero se muestra como ejemplo de cómo se puede usar una distinta con otro SGBD.
                .register("sqlite", SimpleSqlQueryGeneric.class)
                // Para todos los SGBD se usa la implementación genérica.
                .register("*", SimpleSqlQueryGeneric.class)
                .get();
        Class<? extends SimpleSqlQuery> sqlQueryClass = sqlQueryFactory.createSqlQuery(config.getDbUrl());

        // Se define cuáles son las operaciones CRUD que implementarán los DAO.
        DaoProvider daoProvider = new DaoProvider(SimpleListCrud.class, sqlQueryClass);

        // Se defin la fábrica de objetos DAO a partir de los mappers de las entidades.
        // Para la obtención de relaciones se usa carga perezosa (Lazy Loading).
        DaoFactory daoFactory = DaoFactory.Builder.create(daoProvider)
            .registerMapper(CentroMapper.class)
            .registerMapper(EstudianteMapper.class)
            .get(DB_KEY, cp, LoaderFactory.LAZY);

        instance = new Conexion(DB_KEY, cp, daoFactory)
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
            if(!cp.isDatabaseInitialized()) {
                try(Connection conn = cp.getDataSource().getConnection()) {
                    SqlUtils.executeSQL(conn, st);
                    logger.info("Base de datos inicializada correctamente.");
                }
            }
        } catch(SQLException e) {
            throw new DataAccessException("Imposible conectar a la base de datos", e);
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
     * Ejecuta una transacción que devuelve resultados con múltiples DAO.
     * @param operations Código con las operaciones que constituyen la transacción.
     * @throws DataAccessException Si hay un error durante la transacción.
     */
    public <T> T transactionR(TransactionInterfaceR<T> operations) throws DataAccessException {
        //return daoFactory.getTransactionManager().transaction(conn -> { return operations.run(); });
        return TransactionManager.get(DB_KEY).transaction(conn -> { return operations.run(); });
    }

    /**
     * Ejecuta una transacción con múltiples DAO.
     * @param operations Código con las operaciones
     * @throws DataAccessException Si hay un error durante la transacción.
     */
    public void transaction(TransactionInterface operations) throws DataAccessException {
        TransactionManager.get(DB_KEY).transaction(conn -> { operations.run(); });
    }
}