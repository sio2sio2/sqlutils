package edu.acceso.sqlutils.crud;

/**
 * Interfaz para las clases que representan entidades en la base de datos.
 */
public interface Entity {
    /**
     * Obtiene el identificador de la entidad.
     * @return El identificador de la entidad.
     */
    Long getId();

    /**
     * Establece el identificador de la entidad.
     * @param id El identificador a establecer.
     */
    void setId(Long id);
}
