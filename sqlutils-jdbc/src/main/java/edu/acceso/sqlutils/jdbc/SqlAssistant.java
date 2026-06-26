package edu.acceso.sqlutils.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.jdbc.tx.TransactionManager;
import edu.acceso.sqlutils.tx.TransactionContext;

/**
 * Clase auxiliar que permite abreviar la escritura de las operaciones SQL.
 */
public class SqlAssistant {
    private static final Logger logger = LoggerFactory.getLogger(SqlAssistant.class);

    /**
     * Mapa de tipos Java con tipos SQL.
     */
    private static final Map<Class<?>, Integer> TYPEMAP = Map.ofEntries(
        Map.entry(String.class, Types.VARCHAR),
        Map.entry(Integer.class, Types.INTEGER),
        Map.entry(Long.class, Types.BIGINT),
        Map.entry(Double.class, Types.DOUBLE),
        Map.entry(Float.class, Types.FLOAT),
        Map.entry(Boolean.class, Types.BOOLEAN),
        Map.entry(java.math.BigDecimal.class, Types.DECIMAL),
        Map.entry(byte[].class, Types.BLOB),
        Map.entry(java.sql.Time.class, Types.TIME),
        Map.entry(java.sql.Timestamp.class, Types.TIMESTAMP),
        Map.entry(java.sql.Blob.class, Types.BLOB),
        Map.entry(java.sql.Date.class, Types.DATE),
        Map.entry(java.sql.Clob.class, Types.CLOB)
    );

    private final TransactionManager tm;
    private final SqlAssistantConfig config;

    /**
     * Interfaz funcional que representa una operación que acepta tres argumentos y no devuelve ningún resultado.
     * @param <T> Tipo del primer argumento.
     * @param <U> Tipo del segundo argumento.
     * @param <V> Tipo del tercer argumento.
     */
    @FunctionalInterface
    public static interface TriConsumer<T, U, V> {
        void accept(T t, U u, V v);
    }

    /**
     * Registro de configuración para la clase SqlAssistant.
     * @param level Nivel de registro para las consultas SQL.
     * @param logger Función para registrar mensajes de transacción.
     */
    public static record SqlAssistantConfig(boolean log, Level level, TriConsumer<TransactionContext<Connection>, String, String> logger) {

        /**
         * Constructor por defecto de la clase SqlAssistantConfig.
         * Por defecto, se deshabilita el registro de consultas SQL.
         */
        public SqlAssistantConfig() {
            this(false, null, null);
        }

        /**
         * Constructor de la clase SqlAssistantConfig que permite habilitar o deshabilitar el registro de consultas SQL.
         * @param level Nivel de registro para las consultas SQL. Si es {@code null}, se deshabilita el registro de consultas SQL.
         * En caso contrario, para registrar mensajes, se utiliza el logger de la clase SqlAssistant con el nivel especificado,
         * por lo que podrán existir registros engañosos si las transacciones de varias operaciones no se confirman.
         */
        public SqlAssistantConfig(Level level) {
            this(level == null ? false : true, level, null);
        }

        /**
         * Constructor de la clase SqlAssistantConfig.
         * @param logger Función para registrar mensajes que informan de la realización de la operación SQL. Si
         * es {@code null}, se deshabilita el registro de consultas SQL.
         */
        public SqlAssistantConfig(TriConsumer<TransactionContext<Connection>, String, String> logger) {
            this(logger == null ? false : true, Level.TRACE, logger);
        }
    }

    /**
     * Constructor de la clase SqlAssistant.
     * @param tm TransactionManager para manejar las transacciones.
     * @param config Configuración de la clase SqlAssistant.
     */
    public SqlAssistant(TransactionManager tm, SqlAssistantConfig config) {
        Objects.requireNonNull(tm, "El TransactionManager no puede ser nulo");
        this.tm = tm;
        this.config = config == null ? new SqlAssistantConfig() : config;
    }

    /**
     * Constructor de la clase SqlAssistant con configuración por defecto.
     * @param tm TransactionManager para manejar las transacciones.
     */
    public SqlAssistant(TransactionManager tm) {
        this(tm, null);
    }

    /**
     * Clase auxiliar para manejar las claves generadas por una operación SQL.
     * Por simplificación, sólo se maneja un único valor de clave generada.
     */
    public static class KeyHandler {
        /**
         * Lista de claves generadas por la operación SQL.
         */
        private List<Object> ids;

        /**
         * Constructor de la clase KeyHandler.
         */
        public KeyHandler() {
            this.ids = new ArrayList<>(); 
        }

        /**
         * Obtiene las claves generadas por la operación SQL.
         * @return Array de objetos con las claves generadas.
         */
        public Object[] getGeneratedKeys() {
            return ids.toArray();
        }

        /**
         * Agrega una clave generada por la operación SQL.
         * @param lastId Clave generada por la operación SQL.
         */
        public void addGeneratedKey(Object lastId) {
            ids.add(lastId);
        }
    }

    /**
     * Interfaz funcional para mapear una fila de un ResultSet a un objeto de tipo T.
     * @param <T> Tipo de objeto al que se mapeará la fila del ResultSet.
     */
    @FunctionalInterface
    public static interface RowMapper<T> {
        /**
         * Mapea una fila del ResultSet a un objeto de tipo T.
         * @param rs ResultSet que contiene la fila a mapear.
         * @param intRow Número de fila actual en el ResultSet (comenzando desde 0).
         * @return Objeto de tipo T mapeado desde la fila del ResultSet.
         * @throws SQLException Si ocurre un error al acceder a los datos del ResultSet.
         */
        T mapRow(ResultSet rs, Long intRow) throws SQLException;
    }

    /**
     * Interfaz funcional para preparar el objeto {@link PreparedStatement} a partir de una conexión.
     * Permite ejecutar cualquier operación SQL, incluyendo consultas y actualizaciones.
     */
    @FunctionalInterface
    public static interface SqlExecutor {
        /**
         * Prepara un objeto {@link PreparedStatement} a partir de una conexión.
         * @param conn Conexión a la base de datos.
         * @return Objeto {@link PreparedStatement} preparado para ejecutar la operación SQL.
         * @throws SQLException Si ocurre un error al preparar el PreparedStatement.
         */
        PreparedStatement execute(Connection conn) throws SQLException;
    }

    /**
     * Establece un parámetro en un {@link PreparedStatement} a partir de su valor y tipo SQL.
     * @param stmt El objeto PreparedStatement en el que se establecerá el parámetro
     * @param index El índice del parámetro (comenzando desde 1)
     * @param param El valor del parámetro
     * @param sqlType El tipo SQL del parámetro
     * @throws SQLException Si ocurre un error al establecer el parámetro en el PreparedStatement
     */
    private static void setObject(PreparedStatement stmt, int index, Object param, Integer sqlType) throws SQLException {
        if(sqlType != null) {
            stmt.setObject(index, param, sqlType);
            return;
        }

        if(param == null) {
            logger.info("No hay información sobre el tipo del valor nulo, por lo que se prueba a que la base de datos lo averigue.");
            stmt.setNull(index, Types.NULL);
        } else {
            Integer type = TYPEMAP.get(param.getClass());
            if(type != null) {
                stmt.setObject(index, param, type);
            } else if (param instanceof java.util.Date) {
                stmt.setObject(index, new java.sql.Timestamp(((java.util.Date) param).getTime()), Types.TIMESTAMP);
            } else if (param instanceof java.time.LocalDate) {
                stmt.setObject(index, java.sql.Date.valueOf((java.time.LocalDate) param), Types.DATE);
            } else if (param instanceof java.time.LocalTime) {
                stmt.setObject(index, java.sql.Time.valueOf((java.time.LocalTime) param), Types.TIME);
            } else if (param instanceof java.time.LocalDateTime) {
                stmt.setObject(index, java.sql.Timestamp.valueOf((java.time.LocalDateTime) param), Types.TIMESTAMP);
            } else if (param instanceof java.time.OffsetDateTime) {
                stmt.setObject(index, param, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
                logger.warn("Tipo de parámetro no reconocido: {}. Se intentará inferir el tipo SQL a partir del valor", param.getClass().getName());
                stmt.setObject(index, param);
            }
        }
    }

    /**
     * Ejecuta una consulta SQL y mapea los resultados a una lista de objetos de tipo T utilizando un {@link RowMapper}.
     * @param <T> El tipo de los objetos a mapear
     * @param sqlString La cadena SQL de la consulta
     * @param rowMapper El mapeador de filas a objetos
     * @param sqlTypes El array de tipos SQL de los parámetros
     * @param params El array de valores de los parámetros
     * @return Una lista de objetos de tipo T mapeados desde los resultados de la consulta
     */
    public <T> List<T> select(String sqlString, RowMapper<T> rowMapper, Integer[] sqlTypes, Object[] params) {
        Objects.requireNonNull(sqlString, "La cadena SQL no puede ser nula");
        Objects.requireNonNull(params, "El array de parámetros no puede ser nulo");
        Objects.requireNonNull(sqlTypes, "El array de tipos SQL no puede ser nulo");

        if(params.length != sqlTypes.length) {
            throw new IllegalArgumentException("El número de parámetros y el número de tipos SQL deben ser iguales");
        }

        return tm.transaction(ctxt -> {
            Connection conn = ctxt.handle();
            try(PreparedStatement stmt = conn.prepareStatement(sqlString)) {
                for(int i = 0; i < params.length; i++) {
                    setObject(stmt, i + 1, params[i], sqlTypes[i]);
                }
                try(var rs = stmt.executeQuery()) {
                    List<T> result = new java.util.ArrayList<>();
                    long rowNum = 0;
                    while(rs.next()) {
                        result.add(rowMapper.mapRow(rs, rowNum++));
                    }
                    if(config.log) {
                        logger.atLevel(config.level).log("Consulta SQL ejecutada: {}. Número de filas devueltas: {}", sqlString, result.size());
                    }
                    return result;
                }
            } catch (SQLException e) {
                logger.error("Error ejecutando consulta SQL: {}", e.getMessage());
                throw new DataAccessException("Error en la consulta: %s".formatted(e.getMessage()), e);
            }
        });
    }

    /**
     * Ejecuta una consulta SQL y mapea los resultados a una lista de objetos de tipo T utilizando un {@link RowMapper}.
     * <p>No requiere especificar los tipos SQL de los parámetros, ya que se infieren automáticamente a partir de los valores proporcionados,
     * aunque si algún valor es nulo, podría no funcionar correctamente.</p>
     * @param <T> El tipo de los objetos a mapear
     * @param sqlString La cadena SQL de la consulta
     * @param rowMapper El mapeador de filas a objetos
     * @param params El array de valores de los parámetros
     * @return Una lista de objetos de tipo T mapeados desde los resultados de la consulta
     */
    public <T> List<T> select(String sqlString, RowMapper<T> rowMapper, Object ... params) {
        Objects.requireNonNull(sqlString, "La cadena SQL no puede ser nula");
        Objects.requireNonNull(params, "El array de parámetros no puede ser nulo");

        Integer[] sqlTypes = Arrays.stream(params).map(e -> null).toArray(Integer[]::new);

        return select(sqlString, rowMapper, sqlTypes, params);
    }

    /**
     * Ejecuta una consulta SQL que se espera que devuelva un único resultado y mapea ese resultado a un objeto de tipo T utilizando un {@link RowMapper}.
     * @param <T> El tipo del objeto a mapear
     * @param sqlString La cadena SQL de la consulta
     * @param rowMapper El mapeador de filas a objetos
     * @param sqlTypes El array de tipos SQL de los parámetros
     * @param params El array de valores de los parámetros
     * @throws DataAccessException si la consulta devuelve más de un resultado.
     * @return El objeto resultante de la consulta.
     */
    public <T> Optional<T> selectOne(String sqlString, RowMapper<T> rowMapper, Integer[] sqlTypes, Object[] params) {
        List<T> result = select(sqlString, rowMapper, sqlTypes, params);

        return switch(result.size()) {
            case 0 -> Optional.empty();
            case 1 -> Optional.of(result.get(0));
            default -> throw new DataAccessException("Se esperaba un único resultado, pero se obtuvieron %d filas".formatted(result.size()));
        };
    }

    /**
     * Ejecuta una consulta SQL que se espera que devuelva un único resultado y mapea ese resultado a un objeto de tipo T utilizando un {@link RowMapper}.
     * <p>No requiere especificar los tipos SQL de los parámetros, ya que se infieren automáticamente a partir de los valores proporcionados, aunque si algún valor es nulo, podría no funcionar correctamente.</p>
     * @param <T> El tipo de los objetos a mapear
     * @param sqlString La cadena SQL de la consulta
     * @param rowMapper El mapeador de filas a objetos
     * @param params El array de valores de los parámetros
     * @return Un Optional que contiene el objeto de tipo T mapeado desde el resultado de la consulta, o vacío si no se encontraron resultados
     * @throws DataAccessException si la consulta devuelve más de un resultado.
     */
    public <T> Optional<T> selectOne(String sqlString, RowMapper<T> rowMapper, Object ... params) {
        List<T> result = select(sqlString, rowMapper, params);

        return switch(result.size()) {
            case 0 -> Optional.empty();
            case 1 -> Optional.of(result.get(0));
            default -> throw new DataAccessException("Se esperaba un único resultado, pero se obtuvieron %d filas".formatted(result.size()));
        };
    }

    /**
     * Envía un mensaje de error al registro.
     * @param ctxt El contexto de la transacción.
     * @param message El mensaje a enviar.
     * @param notCommittedMessage El mensaje cuando falla la transacción y la transacción no se confirma.
     */
    private void sendMessage(TransactionContext<Connection> ctxt, String message, String notCommittedMessage) {
        if(!config.log) return;
        if(config.logger != null) config.logger.accept(ctxt, message, notCommittedMessage);
        else logger.atLevel(config.level).log(message);
    }

    /**
     * Ejecuta una consulta SQL que no devuelve resultados (por ejemplo, una actualización o eliminación).
     * @param sqlString La cadena SQL de la consulta
     * @param sqlTypes El array de tipos SQL de los parámetros
     * @param params El array de valores de los parámetros
     */
    public void execute(String sqlString, Integer[] sqlTypes, Object[] params) {
        Objects.requireNonNull(sqlString, "La cadena SQL no puede ser nula");
        Objects.requireNonNull(params, "El array de parámetros no puede ser nulo");
        Objects.requireNonNull(sqlTypes, "El array de tipos SQL no puede ser nulo");

        if(params.length != sqlTypes.length) {
            throw new IllegalArgumentException("El número de parámetros y el número de tipos SQL deben ser iguales");
        }

        tm.transaction(ctxt -> {
            Connection conn = ctxt.handle();

            try(PreparedStatement stmt = conn.prepareStatement(sqlString)) {
                for(int i = 0; i < params.length; i++) {
                    setObject(stmt, i + 1, params[i], sqlTypes[i]);
                }
                int rowsAffected = stmt.executeUpdate();
                sendMessage(
                    ctxt,
                    "Operacion SQL ejecutada: %s. Cantidad de final afectadas: %s".formatted(sqlString, rowsAffected),
                    "Transacción fallida: Se cancela la operación SQL '%s'".formatted(sqlString)
                );
            } catch (SQLException e) {
                logger.error("Error ejecutando consulta SQL: {}", e.getMessage());
                throw new DataAccessException("Error en la consulta: %s".formatted(e.getMessage()), e);
            }
        });
    }

    /**
     * Ejecuta una consulta SQL que no devuelve resultados (por ejemplo, una actualización o eliminación).
     * <p>No requiere especificar los tipos SQL de los parámetros, ya que se infieren automáticamente a partir de los valores proporcionados, aunque si algún valor es nulo, podría no funcionar correctamente.</p>
     * @param sqlString La cadena SQL de la consulta
     * @param params El array de valores de los parámetros
     */
    public void execute(String sqlString, Object ... params) {
        Objects.requireNonNull(sqlString, "La cadena SQL no puede ser nula");
        Objects.requireNonNull(params, "El array de parámetros no puede ser nulo");

        Integer[] sqlTypes = Arrays.stream(params).map(e -> null).toArray(Integer[]::new);

        execute(sqlString, sqlTypes, params);
    }

    /**
     * Ejecuta una consulta SQL en lote que no devuelve resultados (por ejemplo, varias actualizaciones o eliminaciones).
     * <p>No requiere especificar los tipos SQL de los parámetros, ya que se infieren automáticamente a partir de los valores proporcionados, aunque si algún valor es nulo, podría no funcionar correctamente.</p>
     * @param sqlString La cadena SQL de la consulta
     * @param sqlTypes El array de tipos SQL de los parámetros
     * @param batchParams El listado de arrays de valores de los parámetros
     * @return Un array con el número de filas afectadas por cada sentencia en el lote
     */
    public int[] executeBatch(String sqlString, Integer[] sqlTypes, List<Object[]> batchParams) {
        Objects.requireNonNull(sqlString, "La cadena SQL no puede ser nula");
        Objects.requireNonNull(batchParams, "El listado de parámetros no puede ser nulo");
        Objects.requireNonNull(sqlTypes, "El array de tipos SQL no puede ser nulo");

        return tm.transaction(ctxt -> {
            Connection conn = ctxt.handle();

            try(PreparedStatement stmt = conn.prepareStatement(sqlString)) {
                for(Object[] params : batchParams) {
                    if(params.length != sqlTypes.length) {
                        throw new IllegalArgumentException("El número de parámetros y el número de tipos SQL deben ser iguales");
                    }
                    for(int i = 0; i < params.length; i++) {
                        setObject(stmt, i + 1, params[i], sqlTypes[i]);
                    }
                    stmt.addBatch();
                }
                int[] rowsAffected = stmt.executeBatch();
                sendMessage(
                    ctxt,
                    "Operacion SQL ejecutada: %s. Resultados: %s".formatted(sqlString, Arrays.toString(rowsAffected)),
                    "Transacción fallida: Se cancela la operación SQL '%s'".formatted(sqlString)
                );
                return rowsAffected;
            } catch (SQLException e) {
                logger.error("Error ejecutando consulta SQL en batch: {}", e.getMessage());
                throw new DataAccessException("Error en la consulta: %s".formatted(e.getMessage()), e);
            }
        });
    }

    /**
     * Ejecuta una consulta SQL en lote que no devuelve resultados (por ejemplo, varias actualizaciones o eliminaciones).
     * <p>No requiere especificar los tipos SQL de los parámetros, ya que se infieren automáticamente a partir de los valores proporcionados, aunque si algún valor es nulo, podría no funcionar correctamente.</p>
     * @param sqlString La cadena SQL de la consulta
     * @param batchParams El listado de arrays de valores de los parámetros
     * @return Un array con el número de filas afectadas por cada sentencia en el lote
     */
    public int[] executeBatch(String sqlString, List<Object[]> batchParams) {
        Objects.requireNonNull(sqlString, "La cadena SQL no puede ser nula");
        Objects.requireNonNull(batchParams, "El listado de parámetros no puede ser nulo");

        Integer[] sqlTypes = new Integer[0];

        if(!batchParams.isEmpty()) {
            sqlTypes = new Integer[batchParams.get(0).length];
            Arrays.fill(sqlTypes, null);
        }

        return executeBatch(sqlString, sqlTypes, batchParams);
    }

    /**
     * Ejecuta una consulta SQL que no devuelve resultados (por ejemplo, una actualización o eliminación).
     * @param executor El código SQL que prepara la sentencia a ejecutar.
     * @param idHandler El manejador de claves generadas por la operación SQL, si se espera que se generen claves.
     */
    public void execute(SqlExecutor executor, KeyHandler idHandler) {
        Objects.requireNonNull(executor, "El ejecutor SQL no puede ser nulo");

        tm.transaction(ctxt -> {
            Connection conn = ctxt.handle();

            try {
                try(PreparedStatement stmt = executor.execute(conn)) {
                    stmt.executeUpdate();
                    sendMessage(
                        ctxt,
                        "Operacion SQL ejecutada",
                        "Transacción fallida: Se cancela la operación SQL"
                    );
                    if(idHandler != null) {
                        try(ResultSet rs = stmt.getGeneratedKeys()) {
                            while(rs.next()) {
                                idHandler.addGeneratedKey(rs.getObject(1));
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                logger.error("Error ejecutando consulta SQL: {}", e.getMessage());
                throw new DataAccessException("Error en la consulta: %s".formatted(e.getMessage()), e);
            }
        });
    }
}