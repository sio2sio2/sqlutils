package edu.acceso.sqlutils.orm.mapper;

/**
 * Interfaz para la traducción de objetos a tipos compatibles con la base de datos.
 */
public interface Translator {
    /**
     * Serializa un objeto a un formato compatible con la base de datos.
     * @param obj El objeto a serializar.
     * @return El valor serializado.
     */
    public Object serialize(Object obj);

    /**
     * Deserializa un valor de la base de datos a un objeto.
     * @param obj El valor a deserializar.
     * @return El objeto deserializado.
     */
    public Object deserialize(Object obj);
}