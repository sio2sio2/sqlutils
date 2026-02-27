package edu.acceso.sqlutils.orm.simple.query;
import edu.acceso.sqlutils.orm.minimal.sql.MinimalSqlQuery;
import edu.acceso.sqlutils.orm.simple.crud.SimpleCrudInterface;

/**
 * Consultas SQL necesarias para implementar {@link SimpleCrudInterface}.
 */
public abstract class SimpleSqlQuery extends MinimalSqlQuery {

    /**
     * Constructor que recibe el nombre de la tabla, el nombre de la columna ID y los nombres de las columnas de la tabla.
     * @param tableName El nombre de la tabla.
     * @param idColumn El nombre de la columna ID.
     * @param columns Los nombres de las columnas de la tabla.
     */
    public SimpleSqlQuery(String tableName, String idColumn, String ... columns) {
        super(tableName, idColumn, columns);
    }

     /**
      * Genera el texto de una consulta SQL para seleccionar un registro por su ID.
      * @return La consulta SQL para seleccionar un registro por su ID.
      */
     public abstract String getSelectIdSql();

    /**
     * Genera el texto de una consulta SQL para seleccionar todos los registros de una tabla.
     * @return La consulta SQL.
     */
    public abstract String getSelectSql();

    /**
     * Genera el texto de una consulta SQL que devuelve los registros uno de cuyos campos es igual a un valor dado.
     * @param column El nombre de la columna.
     * @return La consulta SQL.
     */
    public abstract String getSelectWhereSql(String column);

    /**
     * Genera el texto de una consulta SQL para insertar un nuevo registro en una tabla.
     * @return La consulta SQL.
     */
    public abstract String getInsertSql();

    /**
     * Genera el texto de una consulta SQL para eliminar un registro por su ID.
     * @return La consulta SQL.
     */
    public abstract String getDeleteSql();

    /**
     * Genera el texto de una consulta SQL para actualizar un registro por su ID.
     * @return La consulta SQL.
     */
    public abstract String getUpdateSql();

    /**
     * Genera el texto de una consulta SQL para actualizar el ID de un registro.
     * @return La consulta SQL.
     */
    public abstract String getUpdateIdSql();
}
