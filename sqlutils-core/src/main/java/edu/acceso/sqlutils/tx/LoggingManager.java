package edu.acceso.sqlutils.tx;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

/**
 * Implementa un EventListener que permite deferir los mensajes
 * hasta que se conozca el resultado de la transacción (commit o rollback).
 */
public class LoggingManager extends ContextAwareEventListener {
    /**
     * Clave identificativa del gestor de registros.
     */
    public static final String KEY = new Object().toString();

    /**
     * Modela un mensaje que se pospone hasta que se conozca el resultado de la transacción.
     * @param logger El nombre del logger que se utilizará para registrar el mensaje.
     * @param level El nivel de log (INFO, ERROR, etc.) para el mensaje.
     * @param successMessage El mensaje que se registrará si la transacción se confirma (commit).
     * @param failMessage El mensaje que se registrará si la transacción se revierte (rollback).
     */
    public static record Message(String logger, Level level, String successMessage, String failMessage) {}

    @Override
    public Object createResource() {
        // Esta lista sirve para ir almacenando los mensajes que se desea posponer
        // hasta conocer el resultao de la transacción.
        return new ArrayList<Message>();
    }

    /**
     * Agrega un mensaje a la lista de mensajes a posponer.
     * @param logger El nombre del logger que se utilizará para registrar el mensaje.
     * @param level El nivel de log (INFO, ERROR, etc.) para el mensaje.
     * @param successMessage El mensaje que se registrará si la transacción se confirma (commit).
     * @param failMessage El mensaje que se registrará si la transacción se revierte
     * (rollback).
     */
    public void sendMessage(String logger, Level level, String successMessage, String failMessage) {
        List<Message> messages = getContext().getResource();

        messages.add(new Message(logger, level, successMessage, failMessage));
    }

    /**
     * Agrega un mensaje a la lista de mensajes a posponer utilizando la clase del logger.
     * @param clazz La clase del logger que se utilizará para registrar el mensaje.
     * @param level El nivel de log (INFO, ERROR, etc.) para el mensaje.
     * @param successMessage El mensaje que se registrará si la transacción se confirma (commit).
     * @param failMessage El mensaje que se registrará si la transacción se revierte (rollback).
     */
    public void sendMessage(Class<?> clazz, Level level, String successMessage, String failMessage) {
        sendMessage(clazz.getName(), level, successMessage, failMessage);
    }

    @Override
    public void onCommit() {
        List<Message> messages = getContext().getResource();
        messages.forEach(log -> {
            Logger logger = LoggerFactory.getLogger(log.logger());
            logger.atLevel(log.level()).log(log.successMessage());
        });
    }

    @Override
    public void onRollback() {
        List<Message> messages = getContext().getResource();
        messages.forEach(log -> {
            Logger logger = LoggerFactory.getLogger(log.logger());
            logger.atLevel(log.level()).log(log.failMessage());
        });
    }
}