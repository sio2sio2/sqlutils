package edu.acceso.sqlutils;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import edu.acceso.sqlutils.annotations.Fk;

public class FkLazyLoader<T extends Entity> {

    private static final Map<Class<?>, Map<String, PropertyDescriptor>> cache = new HashMap<>();

    private final Map<String, PropertyDescriptor> descriptors;
    private final Map<String, Integer> fks;
    private T object;

    public FkLazyLoader(T object) {
        this.object = object;
        fks = new HashMap<>();
        descriptors = getPropertyDescriptors();

        // Definimos un mapa con todos las claves foráneas permitidas.
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
                cache.put(clazz, Arrays.stream(descriptors).filter(d -> d.getReadMethod() != null).collect(Collectors.toMap(d -> d.getReadMethod().getName(), d -> d)));
            }
            catch(IntrospectionException err) {
                throw new RuntimeException(err);
            }
        }
        return cache.get(clazz);
    }

    public FkLazyLoader<T> setFk(String name, Integer value) {
        if(!fks.containsKey(name)) throw new IllegalStateException(String.format("%s: Clave foránea inválida", name));
        fks.put(name, value);
        return this;
    }

    @SuppressWarnings("unchecked")
    public T createProxy(Crud<T> sqlDao) {
        return (T) Proxy.newProxyInstance(
            object.getClass().getClassLoader(),
            object.getClass().getInterfaces(),
            new InvocationHandler() {
                @Override
                public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                    PropertyDescriptor pd = descriptors.get(method.getName());
                    Object value = method.invoke(object, args);

                    if(pd != null) {
                        Integer fk = fks.get(pd.getName());  // fk también es nulo si el campo no es una clave foránea.
                        // Si la clave foránea no es nula, pero el atributo al que hace referencia es nulo
                        // es necesario realizar la consulta a la base de datos y establecer el valor.
                        if(fk != null && value == null) {
                            String ent = object.getClass().getSimpleName();
                            String ref = method.getReturnType().getSimpleName();
                            value = sqlDao.get(fk).orElseThrow(() -> new DataAccessException(String.format("Violación de integridad referencial: %s('%d') referido en %s no existe", ref, fk, ent)));
                            pd.getWriteMethod().invoke(object, value);
                        }
                    }
                    return value;
                }
            }
        );
    }
}
