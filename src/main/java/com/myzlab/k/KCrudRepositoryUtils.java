package com.myzlab.k;

import static com.myzlab.k.KFunction.cte;
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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KCrudRepositoryUtils {
    
    final static Logger logger = LoggerFactory.getLogger(KCrudRepositoryUtils.class);
    
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
            logger.error("An error ocurred while getting value from metadata object", e);
            
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
        final Map<Integer, String> columnsDataType,
        final T entity,
        final List<String> dirtyPropertiesOrdered,
        final boolean skipPrimaryKey
    ) {
        final Map<String, KTableColumn> kTableColumns = KCrudRepositoryUtils.getKTableColumns(kMetadataClazz, kTableMetadata);
        
        final Field[] fieldsKRow = kRowClass.getDeclaredFields();
        
        int i = 0;
        
        for (final Field field : fieldsKRow) {
            final KMetadata metadata = field.getAnnotation(KMetadata.class);
            
            if (metadata == null) {
                continue;
            }
            
            final String metadataName = metadata.name();
            final String columnDataType = metadata.columnDataType();
            
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
            
            if (columnDataType != null && !columnDataType.isEmpty()) {
                columnsDataType.put(i, columnDataType);
            }
            
            i++;
            
            field.setAccessible(true);
            
            if (values != null && entity != null) {
                try {
                    final Object o = field.get(entity);

                    values.add(o);
                } catch (Exception e) {
                    logger.error("An error ocurred while getting value from KRow object", e);

                    throw KExceptionHelper.internalServerError(e.getMessage());
                }
            }
            
            columns.add(kTableColumns.get(metadataName));
        }
    }
    
    protected static <T extends KRow> KInsertIntoFilled getKQueryBaseToInsert(
        final String jdbc,
        final KBuilder k,
        final Class<?> kMetadataClazz,
        final KTable kTableMetadata,
        final Class<T> kRowClass,
        final T entity
    ) {
        final List<KTableColumn> columns = new ArrayList<>();
        final List<Object> values = new ArrayList<>();
        final Map<Integer, String> columnsDataType = new HashMap<>();
        
        KCrudRepositoryUtils.getColumnsAndValues(
            kMetadataClazz,
            kTableMetadata,
            kRowClass,
            entity.getDirtyProperties(),
            columns,
            values,
            columnsDataType,
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
            .jdbc(jdbc)
            .insertInto(kTableMetadata)
            .columns(columns.toArray(new KTableColumn[0]))
            .values(kValues, columnsDataType);
    }
    
    protected static <T extends KRow> KInsertIntoFilled getKQueryBaseToInsert(
        final String jdbc,
        final KBuilder k,
        final Class<?> kMetadataClazz,
        final KTable kTableMetadata,
        final Class<T> kRowClass,
        final List<T> entities
    ) {
        final List<KTableColumn> columns = new ArrayList<>();
        final List<String> dirtyPropertiesOrdered = new ArrayList<>();
        final Map<Integer, String> columnsDataType = new HashMap<>();
        
        KCrudRepositoryUtils.getColumnsAndValues(
            kMetadataClazz,
            kTableMetadata,
            kRowClass,
            KCrudRepositoryUtils.getDirtyProperties(entities),
            columns,
            null,
            columnsDataType,
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
                        logger.error("An error ocurred while invoking method from KRow object", e);
                        
                        throw KExceptionHelper.internalServerError(e.getMessage());
                    }
                }

                return list;
            }
        );
        
        return
            k
            .jdbc(jdbc)
            .insertInto(kTableMetadata)
            .columns(columns.toArray(new KTableColumn[0]))
            .values(kValues, columnsDataType);
    }
    
    protected static <T extends KRow> KWhereUpdate getKQueryBaseToUpdate(
        final String jdbc,
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
        final Map<Integer, String> columnsDataType = new HashMap<>();
        
        KCrudRepositoryUtils.getColumnsAndValues(
            kMetadataClazz,
            kTableMetadata,
            kRowClass,
            entity.getDirtyProperties(),
            columns,
            values,
            columnsDataType,
            entity,
            null,
            true
        );
        
        if (columns.isEmpty()) {
            throw KExceptionHelper.internalServerError("At least one column must be manipulated");
        }
        
        final KSetUpdate kSetUpdate =
            k
            .jdbc(jdbc)
            .update(kTableMetadata)
            .set(columns.get(0), values.get(0), columnsDataType.get(0));
        
        for (int i = 1; i < columns.size(); i++) {
            kSetUpdate.set(columns.get(i), values.get(i), columnsDataType.get(i));
        }
        
        return
            kSetUpdate
            .where(kTableColumnId.eq(entity.getPrimaryKeyValue()));
    }
    
    protected static <T extends KRow> KWhereUpdate getKQueryBaseToUpdate(
        final String jdbc,
        final KBuilder k,
        final Class<?> kMetadataClazz,
        final KTable kTableMetadata,
        final Class<T> kRowClass,
        final KTableColumn kTableColumnId,
        final List<T> entities
    ) {
        for (final T entity : entities) {
            if (entity.getPrimaryKeyValue() == null) {
                throw KExceptionHelper.internalServerError("Primary key value is required on all entities");
            }
        }
        
        final List<KTableColumn> columns = new ArrayList<>();
        final List<String> dirtyPropertiesOrdered = new ArrayList<>();
        final Map<Integer, String> columnsDataType = new HashMap<>();
        
        KCrudRepositoryUtils.getColumnsAndValues(
            kMetadataClazz,
            kTableMetadata,
            kRowClass,
            KCrudRepositoryUtils.getDirtyProperties(entities),
            columns,
            null,
            columnsDataType,
            null,
            dirtyPropertiesOrdered,
            false
        );
        
        int indexId = Integer.MAX_VALUE;
        
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).getName().equals(kTableColumnId.getName())) {
                indexId = i;
            }
        }
        
        final List<KTableColumn> columnsWithoutPrimaryKey = 
            columns.stream().filter(c -> !c.getName().equals(kTableColumnId.getName())).collect(Collectors.toList());
        
        if (columnsWithoutPrimaryKey.isEmpty()) {
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
                        logger.error("An error ocurred while invoking method from KRow object", e);
                        
                        throw KExceptionHelper.internalServerError(e.getMessage());
                    }
                }

                return list;
            }
        );

        final List<String> cteColumns = new ArrayList<>();
        
        for (final KTableColumn column : columns) {
            cteColumns.add(column.getName());
        }
        
        final KCommonTableExpressionFilled cte =
            cte("_cte")
            .columns(cteColumns.toArray(new String[cteColumns.size()]))
            .as(kValues, "_");
        
        final KSetUpdate kSetUpdate =
            k
            .jdbc(jdbc)
            .with(cte)
            .update(kTableMetadata)
            .set(
                columnsWithoutPrimaryKey.get(0),
                cte.c(columnsWithoutPrimaryKey.get(0).getName()),
                columnsDataType.get(indexId <= 0 ? 1 : 0)
            );
        
        for (int i = 1; i < columnsWithoutPrimaryKey.size(); i++) {
            kSetUpdate.set(
                columnsWithoutPrimaryKey.get(i),
                cte.c(columnsWithoutPrimaryKey.get(i).getName()),
                columnsDataType.get(indexId <= i ? i + 1 : 0)
            );
        }
        
        return
            kSetUpdate
            .from(cte)
            .where(kTableColumnId.eq(cte.c(kTableColumnId.getName())));
    }
}
