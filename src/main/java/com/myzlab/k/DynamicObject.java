package com.myzlab.k;

import com.myzlab.k.helper.json.JsonHelper;
import java.util.HashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class DynamicObject {
    
    private final DynamicMap dynamicMap;

    private DynamicObject() {
        this.dynamicMap = DynamicMap.create();
    }
    
    public static DynamicObject create() {
        return new DynamicObject();
    }
    
    public DynamicObject add(final String name, final Object value) {
        if (value == null) {
            this.dynamicMap.add(name, null);

            return this;
        }
        
        if (value instanceof KRow) {
            this.dynamicMap.add(name, ((KRow) value).toMap());
                    
            return this;
        }
        
        if (value instanceof KCollection) {
            this.dynamicMap.add(name, ((KCollection) value).toList());
                    
            return this;
        }
        
        this.dynamicMap.add(name, value);
        
        return this;
    }
    
    public DynamicObject addKRowProperties(final KRow kRow) {
        if (kRow == null || kRow.isNull()) {
            return this;
        }
        
        for (final Map.Entry<String, Integer> entry : kRow.ref.entrySet()) {
//            if (kRow.getExclude().contains(entry.getKey())) {
//                continue;
//            }
            
            this.add(entry.getKey(), kRow.o[entry.getValue()]);
        }
        
        return this;
    }
    
//    public DynamicObject addKModelProperties(final KModelDTO kModel) {
//        if (kModel == null) {
//            return this;
//        }
//        
//        final Class myClass = kModel.getEntityClass();
//
//        final Field[] fields = myClass.getDeclaredFields();
//
//        for (final Field field : fields) {
//            final Column column = field.getAnnotation(Column.class);
//
//            if (column == null) {
//                continue;
//            }
//            
//            final String name = column.name();
//            
//            if (!kModel.getFieldsToUpdate().contains(name)) {
//                continue;
//            }
//
//            try {
//                field.setAccessible(true);
//
//                final Object o = field.get(kModel);
//
//                if (!addNullValues) {
//                    if (o == null) {
//                        continue;
//                    }
//                }
//
//                this.add(name, o);
//            } catch (Exception e) {
//                //NOTHING
//            }
//        }
//        
//        return this;
//    }
    
    public DynamicObject addProperties(final Map<String, Object> map) {
        if (map == null) {
            return this;
        }
        
        for (final Map.Entry<String, Object> entry : map.entrySet()) {            
            this.add(entry.getKey(), entry.getValue());
        }
        
        return this;
    }
    
    public ResponseEntity buildResponse() {
        return buildResponse(HttpStatus.OK);
    }
    
    public ResponseEntity buildResponse(HttpStatus httpStatus) {
        return ResponseEntity.status(httpStatus).body(toMap());
    }
    
    public String toJSON() {
        return JsonHelper.toJson(this.toMap());
    }

    public HashMap<String, Object> toMap() {
        return this.dynamicMap.build();
    }
}