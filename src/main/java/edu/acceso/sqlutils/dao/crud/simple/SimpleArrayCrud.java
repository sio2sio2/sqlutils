package edu.acceso.sqlutils.dao.crud.simple;

import java.lang.reflect.Array;
import java.util.Map;
import java.util.stream.Stream;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Versión de SimpleCrud que añade la posibilidad de que los resultados se devuelvan como un array
 * @param <T> Tipo de entidad gestionada
 * */
public class SimpleArrayCrud<T extends Entity> extends SimpleCrud<T> {

    /**
     * Constructor que recibe un clave identificativa y una clase que implementa {@link SimpleSqlQuery}.
     * @param key Clave que identifica la fuente de datos.
     * @param entityClass Clase de la entidad que maneja el DAO.
     * @param mappers Mapa que relaciona las entidades con sus respectivos {@link EntityMapper}.
     * @param sqlQueryClass Clase que implementa {@link SimpleSqlQuery}.
     * @param loaderClass Clase que implementa {@link RelationLoader}.
     */
    public SimpleArrayCrud(String key, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                      Class<? extends SimpleSqlQuery> sqlQueryClass, Class<? extends RelationLoader<? extends Entity>> loaderClass) {
        super(key, entityClass, mappers, sqlQueryClass, loaderClass);
    }

    /**
     * Constructor que crea una nueva instancia de {@link SimpleArrayCrud} a partir de otro {@link SimpleArrayCrud}.
     * 
     * <p>
     * Este objeto {@link SimpleArrayCrud} se construye compartiendo los mismos parámeros que el DAO original
     * que creó el {@link RelationLoader} que se le pasa como argumento. Esto permite conocer
     * cuál es el historial de entidades cargadas y evitar ciclos de referencia.
     * </p>
     * @param originalDao DAO original del que se crea este nuevo DAO.
     * @param rl {@link RelationLoader} que origina este DAO.
     */
    public SimpleArrayCrud(SimpleArrayCrud<? extends Entity> originalDao, RelationLoader<T> rl) {
        super(originalDao, rl);
    }

    @SuppressWarnings("unchecked")
    private T[] createArray(int size) {
        return (T[]) Array.newInstance(entityClass, size); 
    }

    /**
     * Obtiene todas las entidades en forma de array.
     * @return Un array con todas las entidades.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public T[] getArray() throws DataAccessException {
        try(Stream<T> stream = getStream()) {
            return stream.toArray(this::createArray);
        }
    }

    /**
     * Obtiene en forma de array todas las entidades que tenga un determinado campo con un determinado valor.
     * @param field El nombre del campo.
     * @param value El valor del campo.
     * @return Un array con todas las entidades que coinciden con el criterio de búsqueda.
     * @throws DataAccessException Si ocurre un error al acceder a los datos.
     */
    public T[] getArray(String field, Object value) throws DataAccessException {
        try(Stream<T> stream = getStream(field, value)) {
            return stream.toArray(this::createArray);
        }
    }
}
