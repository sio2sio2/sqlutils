package edu.acceso.sqlutils.orm.minimal.sql;

/**
 * Clase abstracta que define la consulta SQL mínimas: obtener
 * una entidad por su ID. Además define el constructor común para
 * todas las clases que definen consultas SQL.
 */
public abstract class MinimalSqlQuery {

    protected final String tableName;
    protected final String idColumn;
    protected final String[] columns;

    /**
     * Constructor
     * @param tableName El nombre de la tabla en la base de datos.
     * @param idColumn El nombre de la columna que representa el ID de la entidad.
     * @param columns Los nombres de las columnas restantes de la tabla.
     */
    public MinimalSqlQuery(String tableName, String idColumn, String ... columns) {
        this.tableName = tableName;
        this.idColumn = idColumn;
        this.columns = columns;
    }

    /**
     * Obtiene la consulta SQL para seleccionar un registro por su ID.
     * @return La consulta SQL para seleccionar un registro por su ID.
     */
     public abstract String getSelectIdSql();
}
