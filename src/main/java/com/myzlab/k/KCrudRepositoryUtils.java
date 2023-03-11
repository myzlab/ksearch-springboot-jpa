package com.myzlab.k;

import static com.myzlab.k.KFunction.values;
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

public class KCrudRepositoryUtils {
    
    protected static Map<String, KTableColumn> getKTableColumns(
        final Class<?> kMetadataClazz,
        final KTable metadata
    ) {
        final Map<String, KTableColumn> kTableColumns = new HashMap<>();
        
        final Field[] fieldsMetadata = kMetadataClazz.getDeclaredFields();
        
        try {
            for (final Field field : fieldsMetadata) {
                if (!field.getType().getSimpleName().equals("KTableColumn")) {
                    continue;
                }

                kTableColumns.put(field.getName(), (KTableColumn) field.get(metadata));
            }
        } catch (Exception e) {
            e.printStackTrace();
            
            throw KExceptionHelper.internalServerError(e.getMessage());
        }
        
        return kTableColumns;
    }
    
    protected static <T extends KRow> List<String> getDirtyProperties(
        final List<T> entities
    ) {
        final Set<String> dirtyProperties = new HashSet<>();
        
        for (final T entity : entities) {
            dirtyProperties.addAll(entity.getDirtyProperties());
        }
        
        return new ArrayList<>(dirtyProperties);
    }
    
    protected static <T extends KRow> void getColumnsAndValues(
        final Class<?> kMetadataClazz,
        final KTable kTableMetadata,
        final Class<T> kRowClass,
        final List<String> dirtyProperties,
        final List<KTableColumn> columns,
        final List<Object> values,
        final T entity,
        final List<String> dirtyPropertiesOrdered,
        final boolean skipPrimaryKey
    ) {
        final Map<String, KTableColumn> kTableColumns = KCrudRepositoryUtils.getKTableColumns(kMetadataClazz, kTableMetadata);
        
        final Field[] fieldsKRow = kRowClass.getDeclaredFields();
        
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
    
    protected static <T extends KRow> KInsertIntoFilled getKQueryBaseToInsert(
        final KBuilder k,
        final Class<?> kMetadataClazz,
        final KTable kTableMetadata,
        final Class<T> kRowClass,
        final T entity
    ) {
        final List<KTableColumn> columns = new ArrayList<>();
        final List<Object> values = new ArrayList<>();
        
        KCrudRepositoryUtils.getColumnsAndValues(
            kMetadataClazz,
            kTableMetadata,
            kRowClass,
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
            k
            .insertInto(kTableMetadata)
            .columns(columns.toArray(new KTableColumn[columns.size()]))
            .values(kValues);
    }
    
    protected static <T extends KRow> KInsertIntoFilled getKQueryBaseToInsert(
        final KBuilder k,
        final Class<?> kMetadataClazz,
        final KTable kTableMetadata,
        final Class<T> kRowClass,
        final List<T> entities
    ) {
        final List<KTableColumn> columns = new ArrayList<>();
        final List<String> dirtyPropertiesOrdered = new ArrayList<>();
        
        KCrudRepositoryUtils.getColumnsAndValues(
            kMetadataClazz,
            kTableMetadata,
            kRowClass,
            KCrudRepositoryUtils.getDirtyProperties(entities),
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
                        final Method methodGet = kRowClass.getMethod(
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
            k
            .insertInto(kTableMetadata)
            .columns(columns.toArray(new KTableColumn[columns.size()]))
            .values(kValues);
    }
    
    protected static <T extends KRow> KWhereUpdate getKQueryBaseToUpdate(
        final KBuilder k,
        final Class<?> kMetadataClazz,
        final KTable kTableMetadata,
        final Class<T> kRowClass,
        final KTableColumn kTableColumnId,
        final T entity
    ) {
        if (entity.getPrimaryKeyValue() == null) {
            throw KExceptionHelper.internalServerError("Primary key value is required");
        }
        
        final List<KTableColumn> columns = new ArrayList<>();
        final List<Object> values = new ArrayList<>();
        
        KCrudRepositoryUtils.getColumnsAndValues(
            kMetadataClazz,
            kTableMetadata,
            kRowClass,
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
            k
            .update(kTableMetadata)
            .set(columns.get(0), values.get(0));
        
        for (int i = 1; i < columns.size(); i++) {
            kSetUpdate.set(columns.get(i), values.get(i));
        }
        
        return
            kSetUpdate
            .where(kTableColumnId.eq(entity.getPrimaryKeyValue()));
    }
}
