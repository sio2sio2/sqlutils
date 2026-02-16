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
public class LoggingManager implements EventListener {
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

    /**
     * Agrega un mensaje a la lista de mensajes a posponer.
     * @param context El contexto de la transacción actual.
     * @param logger El nombre del logger que se utilizará para registrar el mensaje.
     * @param level El nivel de log (INFO, ERROR, etc.) para el mensaje.
     * @param successMessage El mensaje que se registrará si la transacción se confirma (commit).
     * @param failMessage El mensaje que se registrará si la transacción se revierte
     * (rollback).
     */
    public void add(TransactionContext context, String logger, Level level, String successMessage, String failMessage) {
        @SuppressWarnings("unchecked")
        List<Message> messages = (List<Message>) context.getResource(KEY);

        messages.add(new Message(logger, level, successMessage, failMessage));
    }

    /**
     * Agrega un mensaje a la lista de mensajes a posponer utilizando la clase del logger.
     * @param context El contexto de la transacción actual.
     * @param clazz La clase del logger que se utilizará para registrar el mensaje.
     * @param level El nivel de log (INFO, ERROR, etc.) para el mensaje.
     * @param successMessage El mensaje que se registrará si la transacción se confirma (commit).
     * @param failMessage El mensaje que se registrará si la transacción se revierte (rollback).
     */
    public void add(TransactionContext context, Class<?> clazz, Level level, String successMessage, String failMessage) {
        add(context, clazz.getName(), level, successMessage, failMessage);
    }

    @Override
    public void onBegin(EventListenerContext context) {
        context.setResource(new ArrayList<>());
    }

    @Override
    public void onCommit(EventListenerContext context) {
        List<Message> messages = context.getResource();
        messages.forEach(log -> {
            Logger logger = LoggerFactory.getLogger(log.logger());
            logger.atLevel(log.level()).log(log.successMessage());
        });
    }

    @Override
    public void onRollback(EventListenerContext context) {
        List<Message> messages = context.getResource();
        messages.forEach(log -> {
            Logger logger = LoggerFactory.getLogger(log.logger());
            logger.atLevel(log.level()).log(log.failMessage());
        });
    }
}