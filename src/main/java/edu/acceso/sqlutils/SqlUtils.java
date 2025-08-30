package edu.acceso.sqlutils;

import java.sql.Statement;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import edu.acceso.sqlutils.errors.DataAccessRuntimeException;

/**
 * Clase que implementa algunos métodos estáticos adicionales a JDBC.
 */
public class SqlUtils {

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

    /**
     * Descompone un guión SQL en las sentencias de que se compone.
     * @param st Entrada de la que se lee el guión
     * @return  Una lista con las sentencias separadadas.
     * @throws IOException Si ocurre un error al leer el flujo de entrada.
     */
    public static List<String> splitSQL(InputStream st) throws IOException {
        Pattern beginPattern = Pattern.compile("\\b(BEGIN|CASE)\\b", Pattern.CASE_INSENSITIVE);
        Pattern endPattern = Pattern.compile("\\bEND\\b", Pattern.CASE_INSENSITIVE);

        try (
            InputStreamReader sr = new InputStreamReader(st, StandardCharsets.UTF_8);
            BufferedReader br = new BufferedReader(sr);
        ) {
            List<String> sentencias = new ArrayList<>();
            String linea;
            String sentencia = "";
            int contador = 0;
            while((linea = br.readLine()) != null) {
                linea = linea.trim();
                if(linea.isEmpty()) continue;

                Matcher beginMatcher = beginPattern.matcher(linea);
                Matcher endMatcher = endPattern.matcher(linea);

                while(beginMatcher.find()) contador++;
                while(endMatcher.find()) contador--;

                sentencia += "\n" + linea;

                if(contador == 0 && linea.endsWith(";")) {
                    sentencias.add(sentencia);
                    sentencia = "";
                }
            }
            return sentencias;
        }
    }

    /**
     * Ejecuta un guión SQL en la base de datos.
     * @param conn Conexión a la base de datos.
     * @param st Flujo de entrada con el guión SQL.
     * @throws SQLException     Si ocurre un error al ejecutar el guión SQL.
     * @throws IOException      Si ocurre un error al leer el flujo de entrada.
     */
    public static void executeSQL(Connection conn, InputStream st) throws SQLException, IOException {
        conn.setAutoCommit(false);

        try (Statement stmt = conn.createStatement()) {
            for(String sentencia: splitSQL(st)) {
                stmt.addBatch(sentencia);
            }
            stmt.executeBatch();
            conn.commit();
        } catch(SQLException err) {
            conn.rollback();
            throw new SQLException(err);
        } finally {
            conn.setAutoCommit(true);
        }
    }
}
