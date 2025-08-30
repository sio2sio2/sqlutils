package edu.acceso.sqlutils.dao.crud.simple;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import javax.sql.DataSource;

import edu.acceso.sqlutils.crud.Entity;
import edu.acceso.sqlutils.dao.mapper.EntityMapper;
import edu.acceso.sqlutils.dao.relations.RelationLoader;
import edu.acceso.sqlutils.errors.DataAccessException;

/**
 * Versión de SimpleCrud que añade la posibilidad de que los resultados se devuelvan como una lista
 * @param <T> Tipo de entidad que maneja el DAO.
 */
public class SimpleListCrud<T extends Entity> extends SimpleCrud<T> {

    /**
     * Constructor que recibe un {@link DataSource} y una clase que implementa {@link SimpleSqlQuery}.
     * @param ds Una fuente de datos para obtener conexiones a la base de datos.
     * @param entityClass Clase de la entidad que maneja el DAO.
     * @param mappers Mapa que relaciona las entidades con sus respectivos {@link EntityMapper}.
     * @param sqlQueryClass Clase que implementa {@link SimpleSqlQuery}.
     * @param loaderClass Clase que implementa {@link RelationLoader}.
     */
    public SimpleListCrud(DataSource ds, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                      Class<? extends SimpleSqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        super(ds, entityClass, mappers, sqlQueryClass, loaderClass);
    }


    /**
     * Constructor que recibe una {@link Connection} y una clase que implementa {@link SimpleSqlQuery}.
     * @param conn Una conexión a la base de datos.
     * @param entityClass Clase de la entidad que maneja el DAO.
     * @param mappers Mapa que relaciona las entidades con sus respectivos {@link EntityMapper}.
     * @param sqlQueryClass Clase que implementa {@link SimpleSqlQuery}.
     * @param loaderClass Clase que implementa {@link RelationLoader}.
     */
    public SimpleListCrud(Connection conn, Class<T> entityClass, Map<Class<? extends Entity>, EntityMapper<?>> mappers,
                      Class<? extends SimpleSqlQuery> sqlQueryClass, Class<? extends RelationLoader> loaderClass) {
        super(conn, entityClass, mappers, sqlQueryClass, loaderClass);
    }

    /**
     * Constructor que crea una nueva instancia de {@link SimpleListCrud} a partir de otro {@link SimpleListCrud}.
     * 
     * <p>
     * Un {@link SimpleListCrud} obtenido de este modo comparte el cargador de relaciones (véase
     * {@link RelationLoader}) con el original, lo que permite que éste conserve el historial
     * de todas las relaciones cargadas.
     * </p>
     * @param <E> Tipo de entidad del {@link SimpleListCrud} original.
     * @param dao El {@link SimpleListCrud} original a partir del cual se obtiene el nuevo.
     * @param entityClass La clase de la entidad que maneja el nuevo {@link SimpleCrud}.
     */
    public <E extends Entity> SimpleListCrud(SimpleListCrud<E> dao, Class<T> entityClass) {
        super(dao, entityClass);
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
