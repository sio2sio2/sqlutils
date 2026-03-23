package edu.acceso.sqlutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.Runtime.Version;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;

/**
 * Contiene la configuración del programa. Para asegurarnos de su unicidad
 * se usa el patrón Singleton.
 */
@Command
public class Config {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(Config.class);

    /**
     * Instancia única de la clase.
     */
    private static Config instance;

    /** Base de datos predeterminada */
    private static final String DEFAULT_DATABASE = "file::memory:?cache=shared";
    /** SGBD predeterminado */
    private static final String DEFAULT_DBMS = "sqlite";

    private static final String DEFAULT_INPUT = ":resource:/esquema.sql"; // null es la entrada estándar.

   /**
     * Ruta del guión SQL de entrada. null implica que se usará la entrada estándar.
     */
    @Option(names = {"-i", "--input"}, description = "Guíón SQL que construye la base de datos.", converter = ArchivoWrapperConverter.class)
    private ArchivoWrapper input;

    /** Prefijo para determinar el SGBD. */
    @Option(names = {"-B", "--sgbd"}, description = "Sistema de gestión de bases de datos (SGBD).", converter = DbmsSelectorConverter.class)
    private DbmsSelector sgbd;

    /**
     * Nombre de la base de datos.
     */
    @Option(names = {"-b", "--database"}, description = "Nombre o ruta de la base de datos.")
    private String db;

    /** Host de la base de datos. */
    @Option(names = {"-H", "--host"}, description = "Host de la base de datos.")
    private String host;

    /** Puerto de la base de datos. */
    @Option(names = {"-p", "--port"}, description = "Puerto de la base de datos.")
    private Integer port;

    @Option(names = {"-s", "--silent"}, 
            description = "Modo silencioso, sólo muestra errores. Incompatible con -v")
    private boolean silent = false;

    @Option(names = {"-v", "--verbose"}, description = "Incrementa la locuacidad de los registros con cada repetición del argumento. Incompatible con -s")
    private boolean[] verbosity;

    /** Usuario de conexión. */
    @Option(names = {"-u", "--user"}, description = "Usuario de conexión.")
    private String user;

    /** Contraseña de conexión. */
    @Option(names = {"-P", "--password"}, description = "Contraseña de conexión.")
    private String password;

    private Properties properties;

    /**
     * Proveedor de la información de versión.
     */
    private static class VersionProvider implements IVersionProvider {

        private final Properties properties;

        public VersionProvider(Properties properties) {
            this.properties = properties;
        }

        @Override
        public String[] getVersion() throws Exception {
            String appName = properties.getProperty("app.name", "Sin nombre");
            String appVersion = properties.getProperty("app.version", "???");
            String appDescription = properties.getProperty("app.description", "Sin descripción");

            Version runtimeVersion = Runtime.version();
            String osName = System.getProperty("os.name");
            String osVersion = System.getProperty("os.version");
            String osArch = System.getProperty("os.arch");

            return new String[] {
                String.format("%s v%s [JRE v%s. %s v%s (%s)]:", appName, appVersion, runtimeVersion, osName, osVersion, osArch),
                "   " + appDescription,
            };
        }
    }

    /**
     * Excepción interna para peticiones de ayuda. La usa --sgbd.
     */
    private static class HelpRequestException extends Exception {
        private final HelpType helpType;

        public enum HelpType {
            SGBD
        }

        public HelpRequestException(HelpType helpType) {
            this.helpType = helpType;
        }

        public HelpType getHelpType() {
            return helpType;
        }

    }

    private static class ArchivoWrapperConverter implements ITypeConverter<ArchivoWrapper> {
        @Override
        public ArchivoWrapper convert(String value) throws Exception {
            return new ArchivoWrapper(
                switch(value) {
                    case null -> DEFAULT_INPUT;
                    case "-", "stdin" -> null;
                    default -> value;
                }
            );
        }
    }

    private static class DbmsSelectorConverter implements ITypeConverter<DbmsSelector> {
        @Override
        public DbmsSelector convert(String value) throws Exception {
            if("help".equals(value) || "?".equals(value)) {
                throw new HelpRequestException(HelpRequestException.HelpType.SGBD);
            }

            try {
                return DbmsSelector.fromString(value);
            } catch(IllegalArgumentException | UnsupportedOperationException e) {
                throw new IllegalArgumentException(e.getMessage());
            }
        }
    } 

    private Config loadProperties(String resource) {
        properties = new Properties();

        try(
            InputStream st = getClass().getResourceAsStream(resource);
            InputStreamReader sr = new InputStreamReader(st, StandardCharsets.UTF_8);
        ) {
            if(st != null) properties.load(sr);
        } catch (IOException e) {
            logger.warn("No se pudo leer la información de versión.", e);
        }

        return this;
    }

    /**
     * Método estático que controla la creación de la instancia única.
     * @param args Argumentos de la línea de órdenes.
     * @return La instancia creada.
     * @throws IllegalStateException Cuando ya se usó anteriormente el método
     *  y la configuración ya está creada.
     */
    public static Config create(String[] args) {
        if (instance != null) {
            throw new IllegalStateException("La configuración ya fue creada");
        }

        instance = new Config().loadProperties("/app.properties");

        CommandSpec spec = CommandSpec.forAnnotatedObject(instance)
            .name("java -jar sqlutils.jar")
            .mixinStandardHelpOptions(true)
            .versionProvider(new VersionProvider(instance.properties));
        spec.usageMessage().description(instance.properties.getProperty("app.description", "Sin descripción "));

        CommandLine cmd = new CommandLine(spec);

        try {
            cmd.parseArgs(args);

            if (cmd.isUsageHelpRequested()) {
                cmd.usage(System.out);
                System.exit(0);
            }

            if (cmd.isVersionHelpRequested()) {
                cmd.printVersionHelp(System.out);
                System.exit(0);
            }

            // Validaciones y reasignaciones de valor adicionales
            instance.validate();

        } catch(ParameterException e) {
            if(e.getCause() instanceof HelpRequestException hre) {
                switch(hre.getHelpType()) {
                    case SGBD:
                        System.out.println("Sistemas de gestión de bases de datos disponibles:");
                        Arrays.stream(DbmsSelector.values()).forEach(s -> System.out.println(" - " + s.name()));
                        break;
                    default:
                        assert false : "Tipo de ayuda desconocido.";
                }
                System.exit(0);
            }
            else if(e.getCause() instanceof IllegalArgumentException xe) {
                logger.error(xe.getMessage());
                System.exit(1);
            }

            logger.error("Error en parámetros: {}", e.getMessage());
            cmd.usage(System.err);
            System.exit(2);
        }

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

   private void validate() {
        // Validar silent/verbose
        if(silent && verbosity != null && verbosity.length > 0) {
            throw new CommandLine.ParameterException(new CommandLine(this), "Los parámetros -s y -v son incompatibles");
        }

        logger.setLevel(getLogLevel());

        if(input == null) {
            try {
                input = new ArchivoWrapperConverter().convert(null);
                logger.debug("Se usará el guion SQL por defecto: {}", DEFAULT_INPUT);
            } catch (Exception e) {
                assert false : "No se pudo asignar el valor por defecto a la entrada.";
            }
        }

        if(db == null || db.isBlank()) {
            db = DEFAULT_DATABASE;
            sgbd = DbmsSelector.fromString(DEFAULT_DBMS);
            logger.info("No se indicó base de datos. Se usará base y gestor por defecto ({})", sgbd);
        }

        if(sgbd == null) {
            sgbd = DbmsSelector.fromString(DEFAULT_DBMS);
            logger.info("No se indicó SGBD. Se usará el gestor por defecto: {}", sgbd);
        }
   }

    /** 
     * Getter de input 
     * @return El valor de input
     */
    public ArchivoWrapper getInput() {
        return input;
    }

    /**
     * Obtiene el nivel de log configurado.
     * @return El nivel de log.
     */
    public Level getLogLevel() {
        if (silent) return Level.ERROR;

        int verbose = (verbosity == null) ? 0 : verbosity.length;
        return switch (verbose) {
            case 0  -> Level.WARN;
            case 1  -> Level.INFO;
            case 2  -> Level.DEBUG;
            default -> Level.TRACE;
        };
    }

    /**
     * Obtiene la URL para la conexión a la base de datos.
     * @return La URL de conexión.
     */
    public String getDbUrl() {
        return sgbd.getUrl(db, host, port);
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
}
