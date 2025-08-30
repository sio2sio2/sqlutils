package edu.acceso.sqlutils.errors;

/**
 * Define un error de acceso a la base de datos en tiempo de ejecución
 * (es un error no comprobado)
 */
public class DataAccessRuntimeException extends RuntimeException {
    /**
     *  Constructor de la excepción con un mensaje y una causa.
     * @param message Mensaje descriptivo del error.
     * @param cause Causa del error.
     */
    public DataAccessRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
 
    /**
     * Constructor de la excepción con un mensaje.
     * @param cause Causa del error.
     */
    public DataAccessRuntimeException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor de la excepción con un mensaje.
     * @param message Mensaje descriptivo del error.
     */
    public DataAccessRuntimeException(String message) {
        super(message);
    }
}