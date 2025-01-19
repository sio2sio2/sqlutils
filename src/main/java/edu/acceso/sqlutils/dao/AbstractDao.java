package edu.acceso.sqlutils.dao;

import java.sql.Connection;

import javax.sql.DataSource;

/**
 * Clase abstracta de la que deben derivar las clases DAO concretas
 * a fin de que utilicen un DataSource o directamente una Connection
 * para hacer las operaciones con la base de datos. En el primer caso,
 * los métodos que definen operaciones CRUD abren y cierran objetos
 * Connection; en el segundo, no; aunque aparentemente se haga. De este
 * modo, para cualquier implementación podemos suponer que creamos y cerrarmos
 * una conexión, aunque realmente no ocurre siempre tal cosa:
 * <pre>
 *      // Se obtiene una conexión que puede ser nueva (si el DAO se creó
 *      // a partir de un DataSource) o no (si se proporcionó al DAO una
 *      // conexión para su creación)
 *      try(Connection conn = cp.getConnection()) {
 *              // Operamos con la conexión.
 *      }
 *      // Al acabar el bloque, si el ConnectionProvider creó una conexión, se
 *      // cierra, pero si no se creó, no.
 * </pre>
 */
public abstract class AbstractDao {

    protected final ConnectionProvider cp;

    /**
     * Constructor cuando se quiere aprovechar un ConnectionProvider
     * creado por otro objeto DAO. Es útil para resolver las referencias
     * de las claves foráneas.
     * @param cp Un ConnectionProvider ya definido.
     */
    protected AbstractDao(ConnectionProvider cp) {
        this.cp = cp;
    }

    /**
     * Constructor cuando se desea que cada operación CRUD
     * hecha con el objeto DAO cree y cierre su propia conexión.
     * @param ds Un DataSource a partir del cual crear conexiones.
     */
    protected AbstractDao(DataSource ds) {
        cp = ConnectionProvider.fromDataSource(ds);
    }

    /**
     * Constructor cuando se desea que todas las operaciones
     * hechas con el objeto DAO reaprovechan una conexión ya
     * existente. Es útil cuando se desean definir transacciones.
     * @param conn La conexión en la que se quieren hacer las
     *    operaciones CRUD.
     */
    protected AbstractDao(Connection conn) {
        cp = ConnectionProvider.fromConnection(conn);
    }
}
