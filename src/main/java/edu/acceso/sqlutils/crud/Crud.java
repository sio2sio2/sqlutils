package edu.acceso.sqlutils.crud;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz genérica para operaciones CRUD (Crear, Leer, Actualizar, Borrar)
 * sobre entidades de tipo T.
 * @param <T> Tipo de entidad que maneja esta implementación.
 */
public interface Crud<T extends Entity> {

    /**
     * Obtiene una entidad a partir de su identificador.
     * @param id Identificador de la entidad.
     * @return La entidad requerida.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    public Optional<T> get(Long id) throws DataAccessException;

    /**
     * Obtiene la relación completa de entidades de un tipo.
     * @return Una lista con todas las entidades.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    default List<T> get() throws DataAccessException {
        try(Stream<T> stream = getStream()) {
            List<T> entities = stream.toList();
            return entities;
        }
    }

    /**
     * Obtiene un stream de entidades de un tipo.
     * @return Un stream con todas las entidades.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    public Stream<T> getStream() throws DataAccessException;

    /**
     * Obtiene las entidades uno de cuyos campos tiene un valor dado.
     * @param field El nombre del campo por el que filtrar.
     * @param value El valor a buscar. Puede ser cualquier tipo soportado por el traductor de tipos SQL
     * o un objeto {@link Entity} cuyo identificador se usará para la búsqueda.
     * @return La lista de entidades requerida.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    default List<T> get(String field, Object value) throws DataAccessException {
        try(Stream<T> stream = getStream(field, value)) {
            List<T> entities = stream.toList();
            return entities;
        }
    }

    /**
     * Obtiene un stream de entidades uno de cuyos campos tiene un valor dado.
     * @param field El nombre del campo por el que filtrar.
     * @param value El valor a buscar. Puede ser cualquier tipo soportado por el traductor de tipos SQL
     * o un objeto {@link Entity} cuyo identificador se usará para la búsqueda.
     * @return Un stream con las entidades requeridas.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    public Stream<T> getStream(String field, Object value) throws DataAccessException;

    /**
     * Borra una entidad con un determinado identificador.
     * @param id Identificador de la entidad.
     * @return true, si se encontró la entidad y se borró.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    public boolean delete(Long id) throws DataAccessException;
    /**
     * Borra una entidad.
     * @param obj La entidad que se quiere borrar.
     * @return true, si la entidad existía y se borro.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    default boolean delete(T obj) throws DataAccessException {
        return delete(obj.getId());
    }

    /**
     * Agrega una entidad a la base de datos.
     * @param obj La entidad que se quiere agregar.
     * @throws DataAccessException Si hubo algún problema en el acceso o ya existía una entidad con ese identificador.
     */

    public void insert(T obj) throws DataAccessException;
    /**
     * Agrega una multitud de entidades de un determinado tipo a la base de datos.
     * @param objs Las entidades a agregar.
     * @throws DataAccessException Si hubo algún problema en el acceso o ya existía alguna de las entidades.
     */
    default void insert(Iterable<T> objs) throws DataAccessException {
        for(T obj: objs) insert(obj);
    }

    /**
     * Agrega un array de entidades a la base de datos.
     * @param objs Las entidades a agregar.
     * @throws DataAccessException Si hubo algún problema en el acceso o ya existía alguna de las entidades.
     */
    default void insert(T[] objs) throws DataAccessException {
       insert(Arrays.asList(objs));
    }

    /**
     * Actualiza los campos de una entidad cuyo identificador no ha cambiado.
     * @param obj La entidad con los valores actualizados.
     * @return true, si la entidad existía y se pudo actualizar.
     * @throws DataAccessException Si hubo algún problema en el acceso.
     */
    public boolean update(T obj) throws DataAccessException;

    /**
     * Actualiza el identificador de una entidad.
     * @param oldId El valor antiguo del identificador.
     * @param newId El nuevo valor de identificador.
     * @return true, si la entidad existía y se pudo actualizar.
     * @throws DataAccessException Si hubo algún problema en el acceso.
     */
    public boolean update(Long oldId, Long newId) throws DataAccessException;

    /**
     * Actualiza el identificador de una entidad.
     * @param obj La entidad con el identificador sin actualizar. 
     * @param newId El nuevo valor de identificador.
     * @return true, si la entidad existía y se pudo actualizar.
     * @throws DataAccessException Si hubo algún problema en el acceso.
     */
    default boolean update(T obj, Long newId) throws DataAccessException {
        return update(obj.getId(), newId);
    }
}
