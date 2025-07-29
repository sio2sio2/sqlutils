package edu.acceso.sqlutils.errors;

/**
 * Excepción para errores de acceso a datos.
 */
public class DataAccessException extends Exception {
    /**
     *  Constructor de la excepción con un mensaje y una causa.
     * @param message Mensaje descriptivo del error.
     * @param cause Causa del error.
     */
    public DataAccessException(String message, Throwable cause) {
        super(message, cause);
    }
 
    /**
     * Constructor de la excepción con un mensaje.
     * @param cause Causa del error.
     */
    public DataAccessException(Throwable cause) {
        super(cause);
    }

    /**
     * Constructor de la excepción con un mensaje.
     * @param message Mensaje descriptivo del error.
     */
    public DataAccessException(String message) {
        super(message);
    }
}
