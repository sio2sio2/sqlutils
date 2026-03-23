package edu.acceso.sqlutils;

import java.sql.Statement;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.acceso.sqlutils.errors.DataAccessRuntimeException;

/**
 * Clase que implementa algunos métodos estáticos adicionales a JDBC.
 */
public class SqlUtils {
    private static final Logger logger = LoggerFactory.getLogger(SqlUtils.class);

    /**
     * Implementa un iterador a partir de un ResultSet.
     */
    private static class ResultSetIterator implements Iterator<ResultSet> {

        private final ResultSet rs;
        private Statement stmt;
        private boolean avanzar;
        private boolean hasNextElement;

        public ResultSetIterator(Statement stmt, ResultSet rs) {
            this.stmt = stmt;
            this.rs = rs;
            avanzar = true;
        }

        @Override
        public boolean hasNext() {
            if(avanzar) {
                try {
                    if(stmt.isClosed()) {
                        throw new DataAccessRuntimeException(new IllegalStateException("Statement is closed!!!"));
                    }
                    if(rs.isClosed()) {
                        throw new DataAccessRuntimeException("ResultSet is closed!!!");
                    }
                    hasNextElement = rs.next();
                }
                catch(SQLException err) {
                    throw new DataAccessRuntimeException(err);
                }
                finally {
                    avanzar = false;
                }
            }
            return hasNextElement;
        }


        @Override
        public ResultSet next() {
            avanzar = true;
            return rs;
        }
    }
    
    /**
     * Como Function&lt;T, R&gt; pero permite propagar una SQLException.
     * @param <T> El tipo del argumento de la función.
     * @param <R> El tipo del resultado de la función.
     */
    @FunctionalInterface
    public static interface CheckedFunction<T, R> {
        /** Aplica la función
         * @param t El argumento de la función.
         * @return El resultado de la función.
         * @throws SQLException Si ocurre un error al aplicar la función sobre la base de datos.
         */
        R apply(T t) throws SQLException;
    }

    /**
     * Transforma el SQLException que propaga una CheckedFUnction en un DataAccessException, que es una excepción
     * que no necesita ser declarada.
     * @param <T> El tipo que devuelve CheckedFunction.
     * @param checked Un "función" CheckedFunction.
     * @return Una "función" que ha sustituido SQLException por DataAccessException.
     */
    public static <T> Function<ResultSet, T> checkedToUnchecked(CheckedFunction<ResultSet, T> checked) {
        return t -> {
            try {
                return checked.apply(t);
            }
            catch(SQLException err) {
                throw new DataAccessRuntimeException(err);
            }
        };
    }

    /**
     * Genera un flujo con las filas generadas en un ResultSet.
     * @param conn La conexión que generó el {@param ResultSet}.
     * @param stmt La sentencia que generó el {@param ResultSet}.
     * @param rs Los resultados de una consulta.
     * @return Un flujo en el que cada elemento es el siguiente estado del ResultSet proporcionado.
     * @throws DataAccessRuntimeException Cuando se produce un error al acceder a los datos.
     */
    public static Stream<ResultSet> resultSetToStream(Connection conn, Statement stmt, ResultSet rs) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ResultSetIterator(stmt, rs), Spliterator.ORDERED), false)
            .onClose(() -> {
                try {
                    rs.close();
                    if(stmt != null) stmt.close();
                    if(conn != null) conn.close();
                }
                catch(Exception err) {
                    throw new DataAccessRuntimeException(err);
                }
            });
    }

    /**
     * Genera un flujo de objetos derivados del resultado de una consulta.
     * @param <T> La clase del objeto.
     * @param conn La conexión que generó el {@param ResultSet}.
     * @param stmt La sentencia que generó el {@param ResultSet}.
     * @param rs  El objeto que representa los resultado de la consulta.
     * @param mapper La función que permite transformar la fila en un objeto (puede generar un SQLException).
     * @return El flujo de objetos.
     * @throws DataAccessRuntimeException Cuando se produce un error al acceder a los datos.
     */
    public static <T> Stream<T> resultSetToStream(Connection conn, Statement stmt, ResultSet rs, CheckedFunction<ResultSet, T> mapper) {
        return resultSetToStream(conn, stmt, rs).map(checkedToUnchecked(mapper));
    }

    private static enum SqlScriptState {
        NORMAL,
        IN_STRING_LITERAL,
        IN_SINGLE_LINE_COMMENT,
        IN_MULTI_LINE_COMMENT;
    }

    private static enum BlockType {
        BEGIN_END("BEGIN", "END"),
        CASE_END("CASE", "END");

        private final String startKeyword;
        private final String endKeyword;

        private BlockType(String startKeyword, String endKeyword) {
            this.startKeyword = startKeyword;
            this.endKeyword = endKeyword;
        }

        public String getStartKeyword() {
            return startKeyword;
        }

        public String getEndKeyword() {
            return endKeyword;
        }

        public static BlockType fromStartKeyword(String keyword) {
            return Arrays.stream(values())
                .filter(bt -> bt.getStartKeyword().equalsIgnoreCase(keyword))
                .findFirst()
                .orElse(null);
        }
    }

    /**
     * Descompone un guión SQL en las sentencias de que se compone.
     * @param sql Cadena que contiene el guión SQL completo.
     * @return  Una lista con las sentencias separadadas.
     * @throws IOException Si ocurre un error al leer el flujo de entrada.
     * @throws SQLException Si se detecta bloques SQL sin cerrar.
     */
    public static List<String> splitSQL(String sql) throws IOException, SQLException {
        int idx = 0;
        int length = sql.length();
        SqlScriptState state = SqlScriptState.NORMAL;
        StringBuilder currentStatement = new StringBuilder();
        Deque<BlockType> blockStack = new ArrayDeque<>();

        List<String> statements = new ArrayList<>();

        while(idx < length) {
            char c = sql.charAt(idx);

            switch(state) {
                case NORMAL:
                    if(c == '-' && idx + 1 < length && sql.charAt(idx + 1) == '-') {
                        state = SqlScriptState.IN_SINGLE_LINE_COMMENT;
                        idx++;
                    }
                    else if(c == '/' && idx + 1 < length && sql.charAt(idx + 1) == '*') {
                        state = SqlScriptState.IN_MULTI_LINE_COMMENT;
                        idx++;
                    }
                    else if(c == '\'') {
                        state = SqlScriptState.IN_STRING_LITERAL;
                        currentStatement.append(c);
                    }
                    else if(c == ';' && blockStack.isEmpty()) {
                        String statement = currentStatement.toString().trim();
                        if(!statement.isEmpty()) {
                            statements.add(statement);
                        }
                        currentStatement.setLength(0);
                    }
                    else {
                        currentStatement.append(c);

                        // Verificar si se inicia un bloque.
                        if(Character.isLetter(c) && idx + 1 < length && !Character.isLetter(sql.charAt(idx + 1))) {
                            int startIdx = idx;
                            while(startIdx > 0 && Character.isLetter(sql.charAt(startIdx - 1))) {
                                startIdx--;
                            }
                            String keyword = sql.substring(startIdx, idx + 1).toUpperCase();
                            BlockType blockType = BlockType.fromStartKeyword(keyword);
                            if(blockType != null) {
                                blockStack.push(blockType);
                            }
                            else if(!blockStack.isEmpty() && keyword.equalsIgnoreCase(blockStack.peek().getEndKeyword())) {
                                blockStack.pop();
                            }
                        }
                    }
                    break;
                case IN_STRING_LITERAL:
                    currentStatement.append(c);
                    if(c == '\'') {
                        // La comilla está escapada y no marca el final de la cadena.
                        if(idx + 1 < length && sql.charAt(idx + 1) == '\'') {
                            currentStatement.append('\'');
                            idx++;
                        } else {
                            state = SqlScriptState.NORMAL;
                        }
                    }
                    break;
                case IN_SINGLE_LINE_COMMENT:
                    if(c == '\n') {
                        state = SqlScriptState.NORMAL;
                    }
                    break;
                case IN_MULTI_LINE_COMMENT:
                    if(c == '*' && idx + 1 < length && sql.charAt(idx + 1) == '/') {
                        state = SqlScriptState.NORMAL;
                        idx++;
                    }
                    break;
            }
            idx++;
        }
        if(blockStack.size() > 0) throw new SQLException("El script SQL contiene bloques sin cerrar.");
        return statements;
    }

    /**
     * Ejecuta un guión SQL en la base de datos.
     * <p>
     * Si se produce un error, las operaciones se revierten. Desgracidamente,
     * con MariaDB/MySQL las sentencias DDL (CREATE, DROP, ALTER, etc.) provocan un commit implícito,
     * por lo que no es posible revertirlas.
     * @param conn Conexión a la base de datos.
     * @param st Flujo de entrada con el guión SQL.
     * @throws SQLException     Si ocurre un error al ejecutar el guión SQL.
     * @throws IOException      Si ocurre un error al leer el flujo de entrada.
     */
    public static void executeSQL(Connection conn, InputStream st) throws SQLException, IOException {
        boolean originalAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);

        String sqlScript = new String(st.readAllBytes(), StandardCharsets.UTF_8);

        try (Statement stmt = conn.createStatement()) {
            for(String sentencia: splitSQL(sqlScript)) {
                stmt.execute(sentencia);
                logger.debug("Sentencia ejecutada: {}", sentencia);
            }
            if(originalAutoCommit) conn.commit();
        } catch(SQLException err) {
            logger.error("Error al ejecutar el script SQL, se revierten todas las sentencias ejecutadas.", err);
            try {
                conn.rollback();
            } catch(SQLException rollbackErr) {
                err.addSuppressed(rollbackErr);
            }
            throw err;
        } finally {
            conn.setAutoCommit(originalAutoCommit);
        }
    }

    /**
     * Verifica si la base de datos ya ha sido inicializada.
     * @param conn Conexión a la base de datos.
     * @return {@code true} si la base de datos tiene al menos una tabla de usuario, false en caso contrario.
     * @throws SQLException Si ocurre un error al acceder a los metadatos de la base de datos.
     */
    public static boolean isDatabaseEmpty(Connection conn) throws SQLException {
    DatabaseMetaData metaData = conn.getMetaData();

        // 1. Intentamos obtener catálogo y esquema actuales
        String catalog = conn.getCatalog();
        String schema = null;
        
        try {// getSchema() no existe en versiones muy antiguas de JDBC
            schema = conn.getSchema(); 
        } catch (AbstractMethodError | SQLException e) {
            // Evitamos fallos con versiones obsoletas de JDBC
        }

        try (ResultSet tables = metaData.getTables(catalog, schema, "%", new String[]{"TABLE"})) {
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                logger.trace("Hallada tabla en la base de datos: {}. Comprobando si es tabla del sistema...", tableName);
                
                // Filtrar tablas del sistema según el SGBD
                if (!isGenericSystemTable(tableName)) {
                    logger.debug("{} es una tabla de usuario. Se presupone que la base de datos está completamente inicializada.", tableName);
                    return true; // Encontramos al menos una tabla de usuario
                }
            }
        }
        return false;
    }

    /**
     * Verifica si el nombre de la tabla corresponde a una tabla del sistema genérica.
     * @param tableName El nombre de la tabla.
     * @return true si es una tabla del sistema, false en caso contrario.
     */
    private static boolean isGenericSystemTable(String tableName) {
        tableName = tableName.toLowerCase();
        
        // Lista de tablas del sistema comunes en varios SGBD
        return  tableName.startsWith("sys") ||               // Oracle, MSSQL, Derby, DB2, H2
                tableName.contains("information_schema") ||  // MariaDB, MSSQL, MySQL, SQL Server, H2
                tableName.startsWith("databasechangelog") || // Liquibase
                tableName.startsWith("flyway_") ||           // Flyway
                tableName.startsWith("pg_") ||               // PostgreSQL
                tableName.startsWith("msrepl") ||            // MSSQL Replication
                tableName.startsWith("sqlite_");             // SQLite
    }
}
