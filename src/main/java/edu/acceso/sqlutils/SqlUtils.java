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

/**
 * Clase que implementa algunos métodos estáticos adicionales a JDBC.
 */
public class SqlUtils {

    /**
     * Implementa un iterador a partir de un ResultSet.
     */
    private static class ResultSetIterator implements Iterator<ResultSet> {

        private final ResultSet rs;
        private boolean avanzar;
        private boolean hasNextElement;

        public ResultSetIterator(ResultSet rs) {
            this.rs = rs;
            avanzar = true;
        }

        @Override
        public boolean hasNext() {
            if(avanzar) {
                try {
                    if(rs.isClosed()) {
                        throw new DataAccessException("ResultSet is closed!!!");
                    }
                    hasNextElement = rs.next();
                }
                catch(SQLException err) {
                    throw new DataAccessException(err);
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
     * Como Function<T, R> pero permite propagar una SQLException.
     */
    @FunctionalInterface
    public static interface CheckedFunction<T, R> {
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
                throw new DataAccessException(err);
            }
        };
    }

    /**
     * Genera un flujo con las filas generadas en un ResultSet.
     * @param rs Los resutados de una consulta.
     * @return Un flujo en el que cada elemento es el siguiente estado del ResultSet proporcionado.
     * @throws SQLException Cuando se produce un error al realizar la consulta.
     */
    public static Stream<ResultSet> resultSetToStream(Statement stmt, ResultSet rs) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new ResultSetIterator(rs), Spliterator.ORDERED), false)
            .onClose(() -> {
                try {
                    rs.close();
                    stmt.close();
                }
                catch(SQLException err) {
                    throw new DataAccessException(err);
                }
            });
    }

    /**
     * Genera un flujo de objetos derivados del resultado de una consulta.
     * @param <T> La clase del objeto.
     * @param rs  El objeto que representa los resultado de la consulta.
     * @param mapper La función que permite transformar la fila en un objeto (puede generar un SQLException).
     * @return El flujo de objetos.
     * @throws SQLException Cuando Cuando se produce un error al realizar la consulta.
     */
    public static <T> Stream<T> resultSetToStream(Statement stmt, ResultSet rs, CheckedFunction<ResultSet, T> mapper) {
        return resultSetToStream(stmt, rs).map(checkedToUnchecked(mapper));
    }

    /**
     * Descompone un guión SQL en las sentencias de que se compone.
     * @param st Entrada de la que se lee el guión
     * @return  Una lista con las sentencias separadadas.
     * @throws IOException
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

    public static void executeSQL(Connection conn, InputStream st) throws SQLException, IOException {
       try (
           TransactionManager tm = new TransactionManager(conn);
           Statement stmt = conn.createStatement();
       ) {
           List<String> sentencias = splitSQL(st);
           for(String sentencia: sentencias) {
               stmt.addBatch(sentencia);
           }
           stmt.executeBatch();
           tm.commit();
       }
    }
}
