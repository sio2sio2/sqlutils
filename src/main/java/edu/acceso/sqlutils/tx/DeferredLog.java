package edu.acceso.sqlutils.tx;

import org.slf4j.event.Level;

/**
 * Clase que representa un mensaje de log diferido para transacciones.
 * Contiene el nivel de log, el mensaje cuando hubo éxito y el mensaje cuando hubo fallo.
 * @param logger El nombre del logger que se usará para registrar el mensaje.
 * @param level El nivel de log del mensaje.
 * @param successMessage El mensaje a registrar si la transacción se completa con éxito.
 * @param failMessage El mensaje a registrar si la transacción se completa con fallo.
 */
public record DeferredLog(String logger, Level level, String successMessage, String failMessage) {
}
