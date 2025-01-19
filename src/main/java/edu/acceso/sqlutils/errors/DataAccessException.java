package edu.acceso.sqlutils.errors;

/**
 * Define un error de acceso a la fuente de datos.
 */
public class DataAccessException extends Exception {
    public DataAccessException(String message, Throwable cause) {
        super(message, cause);
    }
 
    public DataAccessException(Throwable cause) {
        super(cause);
    }

    public DataAccessException(String message) {
        super(message);
    }
}
