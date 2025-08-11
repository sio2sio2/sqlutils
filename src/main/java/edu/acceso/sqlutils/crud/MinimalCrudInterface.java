package edu.acceso.sqlutils.crud;

import java.util.Optional;

import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Interfaz mínima para operaciones CRUD (Crear, Leer, Actualizar, Borrar)
 * sobre entidades de tipo T. Lo mínimo que deben implementar todas es la obtención de una entidad por su ID,
 * ya que es la operación que se requiere para relacionar entidades mediante claves foráneas.
 * @param <T> Tipo de entidad que maneja esta implementación.
 */
public interface MinimalCrudInterface<T extends Entity> {
    /**
     * Obtiene una entidad a partir de su identificador.
     * @param id Identificador de la entidad.
     * @return La entidad requerida.
     * @throws DataAccessException Si hubo algún problema en el acceso a los datos.
     */
    public Optional<T> get(Long id) throws DataAccessException;
}
