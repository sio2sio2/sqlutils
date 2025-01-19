package edu.acceso.sqlutils.dao;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

import edu.acceso.sqlutils.Entity;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz que define las operaciones CRUD básicas
 * que deben implementar todas las clases DAO.
 */
public interface Crud<T extends Entity> {

    /**
     * Obtiene un objeto a partir de su identificador.
     * @param id El identificador del objeto que se quiere recuperar.
     * @return El objeto recuperado.
     * @throws DataAccessException
     */
    public Optional<T> get(int id) throws DataAccessException;
    /**
     * Obtiene todos los objetos de un determinado tipo.
     * @return EL flujo de objetos.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    public Stream<T> get() throws DataAccessException;

    /**
     * Borrar un objeto del almacenamiento a partir de su identificador.
     * @param id El identificador del objeto.
     * @return true, si se logró borrar el objeto.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    public boolean delete(int id) throws DataAccessException;
    /**
     * Borrar un objeto del almacenamiento.
     * @param obj El objeto que se quiere eliminar.
     * @return true, si se logro borrar el objeto.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    default boolean delete(T obj) throws DataAccessException {
        return delete(obj.getId());
    }

    /**
     * Agrega el objeto al almacenamiento.
     * @param obj El objeto que quiere almacenarse.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    public void insert(T obj) throws DataAccessException;
    /**
     * Agrega varios objeto al almacenamiento.
     * @param objs Los objetos a almacenar.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    default void insert(Iterable<T> objs) throws DataAccessException {
        for(T obj: objs) insert(obj);
    }
    /**
     * Agrega varios objeto al almacenamiento.
     * @param objs Los objetos a almacenar.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    default void insert(T[] obj) throws DataAccessException {
        insert(Arrays.asList(obj));
    }

    /**
     * Actualiza un objeto en el almacenamiento. El identificador NO puede
     * haber cambiado.
     * @param obj El objeto a cambiar.
     * @return true, si se logró hacer la actualización.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    public boolean update(T obj) throws DataAccessException;
    /**
     * Modifica el identificador de un objeto en el almacenamiento.
     * @param oldId El antiguo identificador.
     * @param newId El nuevo identificador.
     * @return true, si se logró hacer la actualización.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    public boolean update(int oldId, int newId) throws DataAccessException;
    /**
     * Modifica el identificador de un objeto en el almacenamiento.
     * @param obj El objeto cuyo identificador se quiere modificar.
     * @param newId EL nuevo identificador.
     * @return true, si se logró la actualización.
     * @throws DataAccessException Cuando se produce un error de acceso.
     */
    default boolean update(T obj, int newId) throws DataAccessException {
        return update(obj.getId(), newId);
    }
}