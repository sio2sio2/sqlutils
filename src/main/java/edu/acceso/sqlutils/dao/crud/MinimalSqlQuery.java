package edu.acceso.sqlutils.dao.crud;

/**
 * Consultas indispensables que debe contener una clase que defina
 * las consultas SQL que se realizan sobre una base de datos.
 */
public interface MinimalSqlQuery {
    /**
     * Obtiene la consulta SQL para seleccionar un registro por su ID.
     * @return La consulta SQL para seleccionar un registro por su ID.
     */
     public String getSelectIdSql();
}
