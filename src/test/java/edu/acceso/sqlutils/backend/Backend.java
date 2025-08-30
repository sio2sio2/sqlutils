package edu.acceso.sqlutils.backend;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.Config;
import edu.acceso.sqlutils.ConnectionPool;
import edu.acceso.sqlutils.SqlUtils;
import edu.acceso.sqlutils.backend.mappers.CentroMapper;
import edu.acceso.sqlutils.backend.mappers.EstudianteMapper;
import edu.acceso.sqlutils.dao.DaoFactory;
import edu.acceso.sqlutils.dao.crud.SimpleCrud;
import edu.acceso.sqlutils.dao.relations.LoaderFactory;
import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.modelo.Centro;


/**
 * Clase principal del backend que inicializa la base de datos y
 * proporciona acceso a la fábrica de DAOs.
 */
public class Backend {
    private static Logger logger = LoggerFactory.getLogger(Backend.class);

    /** Fábrica de DAOs para acceder a las entidades del backend */
    private final DaoFactory daoFactory;
    /** Pool de conexiones a la base de datos */
    private final ConnectionPool cp;

    /**
     * Constructor privado para inicializar el backend con un pool de conexiones y una fábrica de DAOs.
     * @param cp Pool de conexiones a la base de datos.
     * @param daoFactory Fábrica de DAOs para acceder a las entidades del backend.
     */
    private Backend(ConnectionPool cp, DaoFactory daoFactory) {
        this.daoFactory = daoFactory;
        this.cp = cp;
    }

    /**
     * Método estático para crear una instancia de Backend y inicializar la base de datos.
     * @return Una instancia de Backend con la base de datos inicializada.
     * @throws DataAccessException Si ocurre un error al acceder a los datos o al inicializar la base de datos.
     */
    public static DaoFactory createDaoFactory() throws DataAccessException {
        Config config = Config.getInstance();
        ConnectionPool cp = ConnectionPool.getInstance(config.getUrl(), config.getUser(), config.getPassword());
        @SuppressWarnings("unchecked")
        DaoFactory daoFactory = DaoFactory.Builder.create(config.getSqlQueryClass(), SimpleCrud.class)
            .registerMapper(CentroMapper.class)
            .registerMapper(EstudianteMapper.class)
            .get(cp.getDataSource(), LoaderFactory.LAZY);

        return new Backend(cp, daoFactory).inicializar();
    }

    /**
     * Inicializa la base de datos ejecutando un guión SQL.
     * @return La fábrica de DAOs inicializada.
     * @throws DataAccessException Si ocurre un error al acceder a los datos o al inicializar la base de datos.
     */
    private DaoFactory inicializar() throws DataAccessException {
        try {
            // Probamos si existe la tabla de zonas de envío
            daoFactory.getDao(Centro.class).get(1L);
            logger.info("La base de datos ya había sido inicializada.");
        } catch (DataAccessException e) {
            logger.warn("La base de datos no está inicializada.");

            Config config = Config.getInstance();

            try (InputStream st = Files.newInputStream(config.getInput())) {
                SqlUtils.executeSQL(cp.getConnection(), st);
                logger.info("Base de datos inicializada correctamente.");
            } catch (IOException | SQLException err) {
                logger.error("Error al inicializar la base de datos", err);
                throw new DataAccessException("Error al inicializar la base de datos", err);
            }
        }

        return daoFactory;
    }
}
