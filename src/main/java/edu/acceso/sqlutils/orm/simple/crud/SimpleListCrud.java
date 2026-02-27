package edu.acceso.sqlutils.orm.simple.crud;

import java.util.List;
import java.util.stream.Stream;

import edu.acceso.sqlutils.errors.DataAccessException;
import edu.acceso.sqlutils.orm.minimal.Entity;
import edu.acceso.sqlutils.orm.relations.RelationLoader;
import edu.acceso.sqlutils.orm.simple.query.SimpleSqlQuery;

/**
 * Versión de SimpleCrud que añade la posibilidad de que los resultados se devuelvan como una lista
 * @param <T> Tipo de entidad que maneja el DAO.
 */
public class SimpleListCrud<T extends Entity> extends SimpleCrud<T> {

    /**
     * Constructor que recibe una clave identificativa y una clase que implementa {@link SimpleSqlQuery}.
     * @param key La clave que identifica al DaoFactory al que pertenece este DAO.
     * @param entityClass Clase de la entidad que maneja el DAO.
     */
    public SimpleListCrud(String key, Class<T> entityClass) {
        super(key, entityClass);
    }

    /**
     * Constructor que crea una nueva instancia de {@link SimpleListCrud} a partir de otro {@link SimpleListCrud}.
     * 
     * <p>
     * Este objeto {@link SimpleListCrud} se construye compartiendo los mismos parámeros que el DAO original
     * que creó el {@link RelationLoader} que se le pasa como argumento. Esto permite conocer
     * cuál es el historial de entidades cargadas y evitar ciclos de referencia.
     * </p>
     * @param rl {@link RelationLoader} que origina este DAO.
     */
    public SimpleListCrud(RelationLoader<T> rl) {
        super(rl);
    }

    /**
     * Obtiene todas las entidades en forma de lista.
     * @return Una lista con todas las entidades.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public List<T> getList() throws DataAccessException {
        try(Stream<T> stream = getStream()) {
            return stream.toList();
        }
    }

    /**
     * Obtiene en forma de lista todas las entidades que tenga un determinado campo con un determinado valor.
     * @param field El nombre del campo.
     * @param value El valor del campo.
     * @return Una lista con todas las entidades que coinciden con el criterio de búsqueda.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public List<T> getList(String field, Object value) throws DataAccessException {
        try(Stream<T> stream = getStream(field, value)) {
            return stream.toList();
        }
    }
}
