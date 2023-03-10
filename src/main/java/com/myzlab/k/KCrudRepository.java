package com.myzlab.k;

import com.myzlab.k.allowed.KColumnAllowedToReturning;
import com.myzlab.k.allowed.KColumnAllowedToSelect;
import static com.myzlab.k.KFunction.*;
import com.myzlab.k.annotations.Id;
import com.myzlab.k.annotations.KMetadata;
import com.myzlab.k.functions.KValuesFunction;
import com.myzlab.k.helper.KExceptionHelper;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.springframework.http.HttpStatus;

public abstract class KCrudRepository<T extends KRow, Y> {

    public abstract KTable getMetadata();
    public abstract Class<T> getKRowClass();
    public abstract Class<?> getKMetadataClass();
    public abstract KTableColumn getKTableColumnId();
    public abstract KBuilder getK();
    
    public T findById(
        final Y id,
        final KColumnAllowedToSelect... selects
    ) {
        return findById(
            getK().getEntityManagerDefaultName(),
            id,
            selects
        );
    }
    
    public T findById(
        final String jdbc,
        final Y id,
        final KColumnAllowedToSelect... selects
    ) {
        return
            (T)
            getK()
            .jdbc(jdbc)
            .select(selects)
            .from(getMetadata())
            .where(getKTableColumnId().eq(id))
            .single(getKRowClass());
    }
    
    public KCollection<T> findAll(
        final KColumnAllowedToSelect... selects
    ) {
        return findAll(
            getK().getEntityManagerDefaultName(),
            selects
        );
    }
    
    public KCollection<T> findAll(
        final String jdbc,
        final KColumnAllowedToSelect... selects
    ) {
        return
            getK()
            .jdbc(jdbc)
            .select(selects)
            .from(getMetadata())
            .multiple(getKRowClass());
    }
    
    public int deleteById(
        final Y id
    ) {
        return deleteById(
            getK().getEntityManagerDefaultName(),
            id
        );
    }
    
    public int deleteById(
        final String jdbc,
        final Y id
    ) {
        return 
            getK()
            .deleteFrom(getMetadata())
            .where(getKTableColumnId().eq(id))
            .execute();
    }
    
    public KCollection<T> deleteById(
        final Y id,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return deleteById(
            getK().getEntityManagerDefaultName(),
            id,
            kColumnsAllowedToReturning
        );
    }
    
    public KCollection<T> deleteById(
        final String jdbc,
        final Y id,
        final KColumnAllowedToReturning... kColumnsAllowedToReturning
    ) {
        return 
            getK()
            .deleteFrom(getMetadata())
            .where(getKTableColumnId().eq(id))
            .returning(kColumnsAllowedToReturning)
            .execute(getKRowClass());
    }
    
    public boolean existsById(
        final Y id
    ) {
        return existsById(
            getK().getEntityManagerDefaultName(),
            id
        );
    }
    
    public boolean existsById(
        final String jdbc,
        final Y id
    ) {
        final KQuery subQuery =
            getK()
            .select1()
            .from(getMetadata())
            .where(getKTableColumnId().eq(id));

        return
            getK()
            .jdbc(jdbc)
            .select(exists(subQuery).as("GOD_BLESS_YOU"))
            .single()
            .getBoolean(0);
    }
    
    public void assertExistsById(
        final Y id,
        final HttpStatus httpStatus,
        final String message
    ) {
        assertExistsById(
            getK().getEntityManagerDefaultName(),
            id,
            httpStatus,
            message
        );
    }
    
    public void assertExistsById(
        final String jdbc,
        final Y id,
        final HttpStatus httpStatus,
        final String message
    ) {
        final KQuery kQuery =
            getK()
            .select1()
            .from(getMetadata())
            .where(getKTableColumnId().eq(id));
        
        assertExists(getK(), jdbc, kQuery, httpStatus, message);
    }
    
    public void assertNotExistsById(
        final Y id,
        final HttpStatus httpStatus,
        final String message
    ) {
        assertNotExistsById(
            getK().getEntityManagerDefaultName(),
            id,
            httpStatus,
            message
        );
    }
    
    public void assertNotExistsById(
        final String jdbc,
        final Y id,
        final HttpStatus httpStatus,
        final String message
    ) {
        final KQuery kQuery =
            getK()
            .select1()
            .from(getMetadata())
            .where(getKTableColumnId().eq(id));
        
        assertNotExists(getK(), jdbc, kQuery, httpStatus, message);
    }
    
    public long count() {
        return count(getK().getEntityManagerDefaultName());
    }
    
    public long count(
        final String jdbc
    ) {
        return count(jdbc, null);
    }
    
    public long count(
        final KColumn kColumn
    ) {
        return count(getK().getEntityManagerDefaultName(), kColumn);
    }
    
    public long count(
        final String jdbc,
        final KColumn kColumn
    ) {
        return
            getK()
            .jdbc(jdbc)
            .select(kColumn != null ? KFunction.count(kColumn) : KFunction.count())
            .from(getMetadata())
            .single(Long.class);
    }
    
    public long countDistinct(
        final KColumn kColumn
    ) {
        return countDistinct(getK().getEntityManagerDefaultName(), kColumn);
    }
    
    public long countDistinct(
        final String jdbc,
        final KColumn kColumn
    ) {
        return
            getK()
            .jdbc(jdbc)
            .select(KFunction.countDistinct(kColumn))
            .from(getMetadata())
            .single(Long.class);
    }
    //INSERT RETURNING
    public int insert(
        final T entity
    ) {
        final List<KTableColumn> columns = new ArrayList<>();
        final List<Object> values = new ArrayList<>();
        
        getColumnsAndValues(
            entity.getDirtyProperties(),
            columns,
            values,
            entity,
            null,
            false
        );
        
        if (columns.isEmpty()) {
            throw KExceptionHelper.internalServerError("At least one column must be manipulated");
        }
        
        final KValues kValues =
            values().append(values);
        
        return
            getK()
            .insertInto(getMetadata())
            .columns(columns.toArray(new KTableColumn[columns.size()]))
            .values(kValues)
            .execute();
    }
    
    public int insert(
        final List<T> entities
    ) {
        final List<KTableColumn> columns = new ArrayList<>();
        final List<String> dirtyPropertiesOrdered = new ArrayList<>();
        
        getColumnsAndValues(
            getDirtyProperties(entities),
            columns,
            null,
            null,
            dirtyPropertiesOrdered,
            false
        );
        
        if (columns.isEmpty()) {
            throw KExceptionHelper.internalServerError("At least one column must be manipulated");
        }
        
        final KValues kValues = values().append(entities,
            (KValuesFunction<T>) (final T t) -> {
                final List<Object> list = new ArrayList<>();
                
                for (final String property : dirtyPropertiesOrdered) {
                    
                    try {
                        final Method methodGet = getKRowClass().getMethod(
                            "get" + KSearchNameHelper.capitalize(property)
                        );
                        
                        list.add(methodGet.invoke(t));
                    } catch (Exception e) {
                        e.printStackTrace();
                        
                        throw KExceptionHelper.internalServerError(e.getMessage());
                    }
                }

                return list;
            }
        );
        
        return
            getK()
            .insertInto(getMetadata())
            .columns(columns.toArray(new KTableColumn[columns.size()]))
            .values(kValues)
            .execute();
    }
    
    public void update(
        final T entity
    ) {
        if (entity.getPrimaryKeyValue() == null) {
            throw KExceptionHelper.internalServerError("Primary key value is required");
        }
        
        final List<KTableColumn> columns = new ArrayList<>();
        final List<Object> values = new ArrayList<>();
        
        getColumnsAndValues(
            entity.getDirtyProperties(),
            columns,
            values,
            entity,
            null,
            true
        );
        
        if (columns.isEmpty()) {
            throw KExceptionHelper.internalServerError("At least one column must be manipulated");
        }
        
        KSetUpdate kSetUpdate =
            getK()
            .update(getMetadata())
            .set(columns.get(0), values.get(0));
        
        for (int i = 1; i < columns.size(); i++) {
            kSetUpdate.set(columns.get(i), values.get(i));
        }
        
        kSetUpdate
        .where(getKTableColumnId().eq(entity.getPrimaryKeyValue()))
        .execute();
    }
    
    private Map<String, KTableColumn> getKTableColumns() {
        final Map<String, KTableColumn> kTableColumns = new HashMap<>();
        
        final Field[] fieldsMetadata = getKMetadataClass().getDeclaredFields();
        
        try {
            for (final Field field : fieldsMetadata) {
                if (!field.getType().getSimpleName().equals("KTableColumn")) {
                    continue;
                }

                kTableColumns.put(field.getName(), (KTableColumn) field.get(getMetadata()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            
            throw KExceptionHelper.internalServerError(e.getMessage());
        }
        
        return kTableColumns;
    }
    
    private List<String> getDirtyProperties(
        final List<T> entities
    ) {
        final Set<String> dirtyProperties = new HashSet<>();
        
        for (final T entity : entities) {
            dirtyProperties.addAll(entity.getDirtyProperties());
        }
        
        return new ArrayList<>(dirtyProperties);
    }
    
    private void getColumnsAndValues(
        final List<String> dirtyProperties,
        final List<KTableColumn> columns,
        final List<Object> values,
        final T entity,
        final List<String> dirtyPropertiesOrdered,
        final boolean skipPrimaryKey
    ) {
        final Map<String, KTableColumn> kTableColumns = getKTableColumns();
        
        final Field[] fieldsKRow = getKRowClass().getDeclaredFields();
        
        for (final Field field : fieldsKRow) {
            final KMetadata metadata = field.getAnnotation(KMetadata.class);
            
            if (metadata == null) {
                continue;
            }
            
            final String metadataName = metadata.name();
            
            if (skipPrimaryKey) {
                final Id id = field.getAnnotation(Id.class);
                
                if (id != null) {
                    continue;
                }
            }
            
            if (!dirtyProperties.contains(field.getName())) {
                continue;
            }
            
            if (dirtyPropertiesOrdered != null) {
                dirtyPropertiesOrdered.add(field.getName());
            }
            
            field.setAccessible(true);
            
            if (values != null && entity != null) {
                try {
                    final Object o = field.get(entity);

                    values.add(o);
                } catch (Exception e) {
                    e.printStackTrace();

                    throw KExceptionHelper.internalServerError(e.getMessage());
                }
            }
            
            columns.add(kTableColumns.get(metadataName));
        }
    }
    
}
