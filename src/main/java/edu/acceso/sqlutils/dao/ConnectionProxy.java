package edu.acceso.sqlutils.dao;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;

/**
 * Crea un proxy para las conexión con el propósito de interceptar
 * el método close y cerrarlo o no hacerlo dependiendo de lo que se indique
 * al construirlo
 */
class ConnectionProxy implements InvocationHandler {

    private final Connection realConnection;
    private final boolean closeable;

    /**
     * Constructor del proxy
     * @param conn La conexion que se quiere envolver.
     * @param closeable Si se desea cerrar el objeto Connection al invocar 
     *   su método .close().
     */
    private ConnectionProxy(Connection conn, boolean closeable) {
        realConnection = conn;
        this.closeable = closeable;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if(method.getName().equals("close") && !closeable) return null;
        return method.invoke(realConnection, args);
    }

    /**
     * Crea el envoltorio para la conexión.
     * @param connection La conexion que se quiere envolver.
     * @param closeable Si se desea cerrar el objeto Connection al invocar 
     *   su método .close().
     * @return El objeto envuelto.
     */
    public static Connection wrap(Connection connection, boolean closeable) {
        return (Connection) Proxy.newProxyInstance(
            connection.getClass().getClassLoader(),
            new Class<?>[]{Connection.class},
            new ConnectionProxy(connection, closeable)
        );
    }
}