package edu.acceso.sqlutils;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import edu.acceso.sqlutils.annotations.Fk;
import edu.acceso.sqlutils.dao.Crud;
import edu.acceso.sqlutils.errors.DataAccessException;
import javassist.util.proxy.MethodHandler;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.ProxyFactory;

public class FkLazyLoader<T extends Entity> {

    private static final Map<Class<?>, Map<String, PropertyDescriptor>> cache = new HashMap<>();

    private final Map<String, PropertyDescriptor> descriptors;
    private final Map<String, Map<String, Object>> fks;
    private T object;

    public FkLazyLoader(T object) {
        this.object = object;
        fks = new HashMap<>();
        descriptors = getPropertyDescriptors();

        // Definimos un mapa con todos las claves foráneas declaradas.
        for(Field field: object.getClass().getDeclaredFields()) {
            if(!field.isAnnotationPresent(Fk.class)) continue;
            fks.put(field.getName(), null);
        }
    }

    private Map<String, PropertyDescriptor> getPropertyDescriptors() {
        Class<?> clazz = object.getClass();

        // Si no se han cacheado los descriptores de esta clase de objeto.
        if(!cache.containsKey(clazz)) {
            try {
                PropertyDescriptor[] descriptors = Introspector.getBeanInfo(clazz).getPropertyDescriptors();
                cache.put(clazz, Arrays.stream(descriptors).filter(d -> d.getReadMethod() != null).collect(Collectors.toMap(d -> d.getName(), d -> d)));
            }
            catch(IntrospectionException err) {
                throw new RuntimeException(err);
            }
        }
        return cache.get(clazz);
    }

    /**
     * Obtiene un descriptor a partir del nombre de su getter.
     * @param getter El nombre del getter.
     * @return El descriptor asociado al getter.
     */
    private PropertyDescriptor getPropertyDescriptor(String getter) {
        return descriptors.values().stream()
            .filter(d -> d.getReadMethod().getName() == getter).findFirst()
            .orElse(null);
    }

    public FkLazyLoader<T> setFk(String name, Integer fk, Crud<? extends Entity> sqlDao) {
        if(!descriptors.containsKey(name)) {
            throw new IllegalStateException(String.format("%s: no es un atributo o no tiene definido un getter", name));
        }

        if(!fks.containsKey(name)) {
            // Podríamos advertir con un logger de que el atributo no esta anotado como clave foránea.
        }

        // Almacenamos para cada clave foránea el identificador y un objeto apropiado para hacer la consulta.
        Map<String, Object> value = Map.of(
            "id", fk,
            "dao", sqlDao
        );
        fks.put(name, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public T createProxy() {
        ProxyFactory factory = new ProxyFactory();
        factory.setSuperclass(object.getClass());

        MethodHandler handler = (self, method, proceed, args) -> {
            Object value = proceed.invoke(self, args);

            PropertyDescriptor pd = getPropertyDescriptor(method.getName());
            if(pd != null) {  // El método es un getter.
                Map<String, Object> ofk = fks.get(pd.getName());
                // Si la clave foránea no es nula, pero el atributo al que hace referencia es nulo
                // es necesario realizar la consulta a la base de datos y establecer el valor.
                if(ofk != null && value == null) {
                    Integer fk =  (Integer) ofk.get("id"); 
                    Crud<? extends Entity> sqlDao = (Crud<? extends Entity>) ofk.get("dao");
                    String ent = object.getClass().getSimpleName();
                    String ref = method.getReturnType().getSimpleName();
                    value = sqlDao.get(fk).orElseThrow(() -> new DataAccessException(String.format("Violación de integridad referencial: %s('%d') referido en %s no existe", ref, fk, ent)));
                    pd.getWriteMethod().invoke(self, value);
                }
            }

            return value;
        };

        try {
            Object proxy = factory.createClass().getDeclaredConstructor().newInstance();
            ((Proxy) proxy).setHandler(handler);
            return (T) proxy;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            throw new IllegalStateException(String.format("'%s' requiere un constructor sin parámetros", object.getClass().getSimpleName()), e);
        }
    }
}