package edu.acceso.sqlutils;

import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import edu.acceso.sqlutils.query.SqlQuery;
import edu.acceso.sqlutils.query.SqlQueryFactory;

/**
 * Contiene la configuración del programa. Para asegurarnos de su unicidad
 * se usa el patrón Singleton.
 */
public class Config {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(Config.class);

    /**
     * Instancia única de la clase.
     */
    private static Config instance;

    /** Backend por defecto. */
    private static final String DEFAULT_URL = "sqlite:file::memory:?cache=shared";

    /**
     * Ruta del archivo de entrada. null implica que se usará la entrada estándar.
     */
    private Path input;

    /**
     * Interfaz de usuario.
     */
    private String ui;

    /**
     * URL de conexión al backend.
     */
    private String url;

    private Class<? extends SqlQuery> sqlQueryClass;

    /**
     * Nivel de log.
     */
    private Level logLevel = Level.WARN;

    /** Usuario de conexión. */
    private String user;

    /** Contraseña de conexión. */
    private String password;

    /**
     * Constructor privado para evitar su instanciación.
     * @param args Argumentos de la línea de órdenes.
     */
    @SuppressWarnings("null")
    private Config(String[] args) {
        Options options = new Options();
        options.addOption("h", "help", false, "Mostrar ayuda.");
        options.addOption("i", "input", true, "SQL de entrada.");
        options.addOption("u", "url", true, "URL de conexión sin 'jdbc:'. Por defecto, base de datos en memoria SQLite.");
        options.addOption("U", "user", true, "Usuario de conexión. Por defecto, null.");
        options.addOption("P", "password", true, "Contraseña de conexión. Por defecto, null.");
        options.addOption("v", "verbose", false, "Aumenta la verbosidad de los mensajes. Puede repetirse.");
        options.addOption("q", "quiet", false, "Elimina mensajes de depuración: sólo deja los de error.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error al analizar los argumentos: " + e.getMessage());
            System.exit(1);
        }

        if(cmd.hasOption("help")) {
            new HelpFormatter().printHelp(
                "java -jar tarea_4_1.jar",
                "Almacena el CSV de profesores en otros formatos",
                options,
                "Ejemplo: java -jar tarea_4_1.jar -i pedidos.sql -I console",
                true
            );
            System.exit(0);
        }

        // Nivel de depuración
        if(cmd.hasOption("verbose")) {
            int verbosity = (int) Arrays.stream(cmd.getOptions())
                .filter(o -> o.getLongOpt().equals("verbose")).count();
            logLevel = switch(verbosity) {
                case 1 -> Level.INFO;
                case 2 -> Level.DEBUG;
                default -> Level.TRACE;
            };
        }
        else if(cmd.hasOption("quiet")) {
            logLevel = Level.ERROR;
        }

        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(logLevel);
        logger.debug("Nivel de los registros establecido a {}", logLevel);

        String inputFile = cmd.getOptionValue("input");
        if(inputFile == null) {
            logger.error("Debe especificarse un archivo de entrada.");
            System.exit(2);
        }
        else {
            input =Path.of(inputFile);
            if(!Files.exists(input)) { // Comprobamos si está en resources
                try {
                    String iF = input.isAbsolute()?inputFile:"/" + inputFile;
                    input = Path.of(getClass().getResource(iF).toURI());
                    logger.debug("Intentamos comprobar si el archivo existe en resources");
                }
                catch(NullPointerException e) {
                    logger.error("{}: archivo inexistente", inputFile);
                    System.exit(3);
                }
                catch(URISyntaxException e) {
                    logger.error("{}: archivo inválido", inputFile);
                    System.exit(2);
                }
            }
        }

        url = cmd.getOptionValue("url", DEFAULT_URL);
        if(!url.startsWith("jdbc:")) url = "jdbc:" + url;
        sqlQueryClass = SqlQueryFactory.getInstance().createSqlQuery(url);
        logger.debug("URL de conexión al backend: {}", url);
    }

    /**
     * Método estático que controla la creación de la instancia única.
     * @param args Argumentos de la línea de órdenes.
     * @return La instancia creada.
     * @throws IllegalStateException Cuando ya se usó anteriormente el método
     *  y la configuración ya está creada.
     */
    public static Config create(String[] args) {
        if(instance == null) instance = new Config(args);
        else throw new IllegalStateException("La configuración ya fue creada");

        return instance;
    }

    /**
     * Método estático para recuperar la instancia única de configuración.
     * @return La instancia con la configuración.
     */
    public static Config getInstance() {
        if(instance == null) throw new IllegalStateException("La configuración no ha sido creada");
        return instance;
    }

    /** 
     * Getter de input 
     * @return El valor de input
     */
    public Path getInput() {
        return input;
    }

    /** 
     * Getter de ui 
     * @return El valor de ui
     */
    public String getUi() {
        return ui;
    }

    /** 
     * Getter de backend 
     * @return El valor de backend
     */
    public String getUrl() {
        return url;
    }
    
    /** 
     * Getter de logLevel 
     * @return El valor de logLevel
     */
    public Level getLogLevel() {
        return logLevel;
    }

    /** 
     * Getter de user 
     * @return El valor de user
     */
    public String getUser() {
        return user;
    }

    /** 
     * Getter de password 
     * @return El valor de password
     */
    public String getPassword() {
        return password;
    }

    /** 
     * Getter de sqlQueryClass 
     * @return El valor de sqlQueryClass
     */
    public Class<? extends SqlQuery> getSqlQueryClass() {
        return sqlQueryClass;
    }
}
