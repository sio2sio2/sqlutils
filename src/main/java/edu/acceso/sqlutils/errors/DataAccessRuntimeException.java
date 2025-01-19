package edu.acceso.sqlutils.errors;

/**
 * Define un error de acceso a la base de datos en tiempo de ejecución
 * (es un error no comprobado)
 */
public class DataAccessRuntimeException extends RuntimeException {
    public DataAccessRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
 
    public DataAccessRuntimeException(Throwable cause) {
        super(cause);
    }

    public DataAccessRuntimeException(String message) {
        super(message);
    }
}