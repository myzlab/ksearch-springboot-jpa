package com.myzlab.k;

import com.myzlab.k.functions.KRowFunction;
import com.myzlab.k.helper.KExceptionHelper;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class KRow {
    
    protected Object[] o;
    protected Map<String, Integer> ref;
    protected boolean isNull = false;
    
    public KRow() {}

    public KRow(
        final Object[] o,
        final Map<String, Integer> ref
    ) {
        this.o = o;
        this.ref = ref;
    }
    
    public KRow(
        final Object[] o,
        final Map<String, Integer> ref,
        final boolean isNull
    ) {
        this.o = o;
        this.ref = ref;
        this.isNull = isNull;
    }
    
    public Object get(
        final KTableColumn kTableColumn
    ) {
        return this.get(kTableColumn.name);
    }
    
    public Object get(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.get(n);
    }
    
    public Object get(
        final int n
    ) {
        if (n >= o.length) {
            return null;
        }
        
        return o[n];
    }
    
    public String getString(
        final KTableColumn kTableColumn
    ) {
        return this.getString(kTableColumn.name);
    }
    
    public String getString(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getString(n, c);
    }
    
    public String getString(
        final int n
    ) {
        return this.getString(n, null);
    }
    
    private String getString(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (String) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "String");
            
            return null;
        }
    }
    
    public Character getCharacter(
        final KTableColumn kTableColumn
    ) {
        return this.getCharacter(kTableColumn.name);
    }
    
    public Character getCharacter(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getCharacter(n, c);
    }
    
    public Character getCharacter(
        final int n
    ) {
        return this.getCharacter(n, null);
    }
    
    private Character getCharacter(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (Character) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Character");
            
            return null;
        }
    }
    
    public UUID getUUID(
        final KTableColumn kTableColumn
    ) {
        return this.getUUID(kTableColumn.name);
    }
    
    public UUID getUUID(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getUUID(n, c);
    }
    
    public UUID getUUID(
        final int n
    ) {
        return this.getUUID(n, null);
    }
    
    private UUID getUUID(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof String) {
            return UUID.fromString((String) o[n]);
        }
        
        try {
            return (UUID) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "UUID");
            
            return null;
        }
    }
    
    public BigDecimal getBigDecimal(
        final KTableColumn kTableColumn
    ) {
        return this.getBigDecimal(kTableColumn.name);
    }
    
    public BigDecimal getBigDecimal(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBigDecimal(n, c);
    }
    
    public BigDecimal getBigDecimal(
        final int n
    ) {
        return this.getBigDecimal(n, null);
    }
    
    private BigDecimal getBigDecimal(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (BigDecimal) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "BigDecimal");
            
            return null;
        }
    }
    
    public BigInteger getBigInteger(
        final KTableColumn kTableColumn
    ) {
        return this.getBigInteger(kTableColumn.name);
    }
    
    public BigInteger getBigInteger(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBigInteger(n, c);
    }
    
    public BigInteger getBigInteger(
        final int n
    ) {
        return this.getBigInteger(n, null);
    }
    
    private BigInteger getBigInteger(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (BigInteger) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "BigInteger");
            
            return null;
        }
    }
    
    public Long getLong(
        final KTableColumn kTableColumn
    ) {
        return this.getLong(kTableColumn.name);
    }
    
    public Long getLong(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getLong(n, c);
    }
    
    public Long getLong(
        final int n
    ) {
        return this.getLong(n, null);
    }
    
    private Long getLong(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof BigInteger) {
            return ((BigInteger) o[n]).longValue();
        }
        
        try {
            return (Long) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Long");
            
            return null;
        }
    }
    
    public Integer getInteger(
        final KTableColumn kTableColumn
    ) {
        return this.getInteger(kTableColumn.name);
    }
    
    public Integer getInteger(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getInteger(n, c);
    }
    
    public Integer getInteger(
        final int n
    ) {
        return this.getInteger(n, null);
    }
    
    private Integer getInteger(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof Short) {
            return ((Short) o[n]).intValue();
        }
        
        if (o[n] instanceof BigInteger) {
            return ((BigInteger) o[n]).intValue();
        }
        
        try {
            return (Integer) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Integer");
            
            return null;
        }
    }
    
    public Boolean getBoolean(
        final KTableColumn kTableColumn
    ) {
        return this.getBoolean(kTableColumn.name);
    }
    
    public Boolean getBoolean(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBoolean(n, c);
    }
    
    public Boolean getBoolean(
        final int n
    ) {
        return this.getBoolean(n, null);
    }
    
    private Boolean getBoolean(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (Boolean) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Boolean");
            
            return null;
        }
    }
    
    public Double getDouble(
        final KTableColumn kTableColumn
    ) {
        return this.getDouble(kTableColumn.name);
    }
    
    public Double getDouble(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getDouble(n, c);
    }
    
    public Double getDouble(
        final int n
    ) {
        return this.getDouble(n, null);
    }
    
    private Double getDouble(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof BigDecimal) {
            return ((BigDecimal) o[n]).doubleValue();
        }

        try {
            return (Double) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Double");
            
            return null;
        }
    }
    
    public byte[] getBytea(
        final KTableColumn kTableColumn
    ) {
        return this.getBytea(kTableColumn.name);
    }
    
    public byte[] getBytea(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getBytea(n, c);
    }
    
    public byte[] getBytea(
        final int n
    ) {
        return this.getBytea(n, null);
    }
    
    private byte[] getBytea(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }

        try {
            return (byte[]) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "byte[]");
            
            return null;
        }
    }
    
    public LocalDateTime getLocalDateTime(
        final KTableColumn kTableColumn
    ) {
        return this.getLocalDateTime(kTableColumn.name);
    }
    
    public LocalDateTime getLocalDateTime(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getLocalDateTime(n, c);
    }
    
    public LocalDateTime getLocalDateTime(
        final int n
    ) {
        return this.getLocalDateTime(n, null);
    }
    
    public LocalDateTime getLocalDateTime(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof Timestamp) {
            return ((Timestamp) o[n]).toLocalDateTime();
        }
        
        try {
            return (LocalDateTime) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "LocalDateTime");
            
            return null;
        }
    }
    
    public LocalDate getLocalDate(
        final KTableColumn kTableColumn
    ) {
        return this.getLocalDate(kTableColumn.name);
    }
    
    public LocalDate getLocalDate(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getLocalDate(n, c);
    }
    
    public LocalDate getLocalDate(
        final int n
    ) {
        return this.getLocalDate(n, null);
    }
    
    public LocalDate getLocalDate(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        if (o[n] == null) {
            return null;
        }
        
        if (o[n] instanceof Date) {
            return ((Date) o[n]).toLocalDate();
        }
        
        try {
            return (LocalDate) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "LocalDate");
            
            return null;
        }
    }
    
    public Date getDate(
        final KTableColumn kTableColumn
    ) {
        return this.getDate(kTableColumn.name);
    }
    
    public Date getDate(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getDate(n, c);
    }
    
    public Date getDate(
        final int n
    ) {
        return this.getDate(n, null);
    }
    
    public Date getDate(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (Date) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Date");
            
            return null;
        }
    }

    public Timestamp getTimestamp(
        final KTableColumn kTableColumn
    ) {
        return this.getTimestamp(kTableColumn.name);
    }
    
    public Timestamp getTimestamp(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getTimestamp(n, c);
    }
    
    public Timestamp getTimestamp(
        final int n
    ) {
        return this.getTimestamp(n, null);
    }
    
    public Timestamp getTimestamp(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }
        
        try {
            return (Timestamp) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Timestamp");
            
            return null;
        }
    }
    
    public Object[] getArray(
        final KTableColumn kTableColumn
    ) {
        return this.getArray(kTableColumn.name);
    }
    
    public Object[] getArray(
        final String c
    ) {
        final Integer n = ref.get(c);
        
        if (n == null) {
            return null;
        }
        
        return this.getArray(n, c);
    }
    
    public Object[] getArray(
        final int n
    ) {
        return this.getArray(n, null);
    }
    
    private Object[] getArray(
        final int n,
        final String nameRef
    ) {
        if (n >= o.length) {
            return null;
        }

        try {
            return (Object[]) o[n];
        } catch (Exception e) {
            throwNoCasteableMessage(n, nameRef, "Object[]");
            
            return null;
        }
    }
    
    protected boolean isPresent(
        final KTableColumn kTableColumn
    ) {
        return this.isPresent(kTableColumn.name);
    }
    
    protected boolean isPresent(
        final String c
    ) {
        if (o == null) {
            return false;
        }
        
        return ref.get(c) != null;
    }
    
    public boolean isNull() {
        return isNull;
    }
    
    public boolean isNull(
        final KTableColumn kTableColumn
    ) {
        return this.isNull(kTableColumn.name);
    }
    
    public boolean isNull(
        final String c
    ) {
        if (o == null) {
            return true;
        }
        
        final Integer n = ref.get(c);
        
        if (n == null) {
            return true;
        }
        
        return isNull(n);
    }
    
    public boolean isNull(
        final int n
    ) {
        if (o == null) {
            return true;
        }
        
        if (n >= o.length) {
            return true;
        }
        
        return o[n] == null;
    }
    
    public boolean isNullOrEmpty(
        final KTableColumn kTableColumn
    ) {
        return this.isNullOrEmpty(kTableColumn.name);
    }
    
    public boolean isNullOrEmpty(
        final String c
    ) {
        if (o == null) {
            return true;
        }
        
        final Integer n = ref.get(c);
        
        if (n == null) {
            return true;
        }
        
        return isNullOrEmpty(n);
    }
    
    public boolean isNullOrEmpty(
        final int n
    ) {
        if (o == null) {
            return true;
        }
        
        if (n >= o.length) {
            return true;
        }
        
        if (o[n] == null) {
            return true;
        }
        
        return ((String) o[n]).trim().isEmpty();
    }
    
    public <T extends KRow> T assertNotNull(
        final KTableColumn kTableColumn,
        final HttpStatus status,
        final String message
    ) {
        if (this.isNull) {
            throw new KException(status, message);
        }
        
        if (this.isNull(kTableColumn.name)) {
            throw new KException(status, message);
        }
        
        return (T) this;
    }
    
    public <T extends KRow> T assertNotNull(
        final String c,
        final HttpStatus status,
        final String message
    ) {
        if (this.isNull) {
            throw new KException(status, message);
        }
        
        if (this.isNull(c)) {
            throw new KException(status, message);
        }
        
        return (T) this;
    }
    
    public <T extends KRow> T assertNotNull(
        final int n,
        final HttpStatus status,
        final String message
    ) {
        if (this.isNull) {
            throw new KException(status, message);
        }
        
        if (this.isNull(n)) {
            throw new KException(status, message);
        }
        
        return (T) this;
    }
    
    public <T extends KRow> T assertNotNull(
        final String c,
        final HttpStatus status
    ) {
        return (T) assertNotNull(c, status, null);
    }
    
    public <T extends KRow> T assertNotNull(
        final int n,
        final HttpStatus status
    ) {
        return assertNotNull(n, status, null);
    }
    
    public <T extends KRow> T assertNotNull(
        final HttpStatus status,
        final String message
    ) {
        if (this.isNull) {
            throw new KException(status, message);
        }
        
        return (T) this;
    }
    
    public <T extends KRow> T assertNotNull(
        final HttpStatus status
    ) {
        return (T) assertNotNull(status, null);
    }
    
    public <T extends KRow> T set(
        final KTableColumn kTableColumn,
        final Object o
    ) {
        final Integer n = ref.get(kTableColumn.name);
        
        if (n == null) {
            throw KExceptionHelper.internalServerError("Property '" + kTableColumn.name + "' used in 'set' method not found");
        }
        
        this.o[n] = o;
        
        return (T) this;
    }
    
    public <T extends KRow> T set(
        final String property,
        final Object o
    ) {
        final Integer n = ref.get(property);
        
        if (n == null) {
            throw KExceptionHelper.internalServerError("Property '" + property + "' used in 'set' method not found");
        }
        
        this.o[n] = o;
        
        return (T) this;
    }
    
    public <T extends KRow> T set(
        final KTableColumn kTableColumn,
        final KRowFunction kRowFunction
    ) {
        final Integer n = ref.get(kTableColumn.name);
        
        if (n == null) {
            throw KExceptionHelper.internalServerError("Property '" + kTableColumn.name + "' used in 'set' method not found");
        }
        
        if (kRowFunction == null) {
            this.o[n] = null;
            
            return (T) this;
        }
        
        this.o[n] = kRowFunction.run(this);
        
        return (T) this;
    }
    
    public <T extends KRow> T set(
        final String property,
        final KRowFunction kRowFunction
    ) {
        final Integer n = ref.get(property);
        
        if (n == null) {
            throw KExceptionHelper.internalServerError("Property '" + property + "' used in 'set' method not found");
        }
        
        if (kRowFunction == null) {
            this.o[n] = null;
            
            return (T) this;
        }
        
        this.o[n] = kRowFunction.run(this);
        
        return (T) this;
    }
    
//    public Map<String, Object> toMap() {
//        return toMap(new ArrayList<>());
//    }
    
    public Map<String, Object> toMap(
        final String property
    ) {
        if (property == null) {
            return toMap();
        }
        
        return toMap();//new ArrayList() {{ add(property); }}
    }
    
    public Map<String, Object> toMap(
        //final List<String> exclude
    ) {
        if (this.isNull) {
            return null;
        }
        
//        final List<String> listExclude = new ArrayList<>(this.exclude);
//        
//        if (exclude != null && !exclude.isEmpty()) {
//            listExclude.addAll(exclude);
//        }
        
        final Map<String, Object> map = new HashMap<>();
        
        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
            final String key = entry.getKey();
            
//            if (listExclude.contains(key)) {
//                continue;
//            }
            
            if (o[entry.getValue()] == null) {
                continue;
            }
            
            final Object value = o[entry.getValue()];

            if (value instanceof List) {
                final List list = (List) value;

                if (list.isEmpty()) {
                    map.put(key, value);
                    
                    continue;
                }
                
                if (list.get(0) instanceof KRow) {
                    final List<Map<String, Object>> kRows = new ArrayList<>();

                    for (final Object o_ : list) {
                        kRows.add(((KRow) o_).toMap());
                    }

                    map.put(key, kRows);
                } else {
                    map.put(key, value);
                }
                
                continue;
            }
                
            map.put(key, value);
        }
        
        return map;
    }
    
    private Map<String, Object> toResponse() {
        if (this.isNull) {
            return null;
        }
        
        final Map<String, Object> map = new HashMap<>();
        
        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
//            if (this.exclude.contains(entry.getKey())) {
//                continue;
//            }
            
//            if (o[entry.getValue()] != null) {
                map.put(entry.getKey(), o[entry.getValue()]);   
//            }
        }
        
        return map;
//        return new JSONObject(map).toString();
    }
    
    public ResponseEntity buildResponse() {
        return ResponseEntity.ok(this.toResponse());
    }
    
    private void addColumn(
        final String name,
        final Object value
    ) {
        this.o = Arrays.copyOf(o, o.length + 1);
        this.o[o.length - 1] = value;
        
        this.ref.put(name, o.length - 1);
    }
    
    public KRow removeProperty(
        final KTableColumn kTableColumn
    ) {
        return this.removeProperty(kTableColumn.name);
    }
    
    public KRow removeProperty(
        final String name
    ) {
        if (!this.isPresent(name)) {
            throw KExceptionHelper.internalServerError("The column in KRow [" + name + "] to be removed does not exist. Ref? [" + ref + "]");
        }
        
        final Object[] previous = this.o.clone();
        this.o = new Object[this.o.length - 1];
        
        final Integer deleted = this.ref.get(name);
        int current = 0;
        
        for (int i = 0; i < previous.length; i++) {
            if (i != deleted) {
                this.o[current++] = previous[i];
            }
        }
        
        this.ref.remove(name);
        
        for (final Map.Entry<String, Integer> entry : this.ref.entrySet()) {
            final Integer currentValue = entry.getValue();
            
            if (currentValue > deleted) {
                this.ref.put(entry.getKey(), currentValue - 1);
            }
        }
        
        return this;
    }
    
    public KRow addProperty(
        final String property,
        final KRowFunction kRowFunction
    ) {
        this.addColumn(property, kRowFunction.run(this));
        
        return this;
    }
    
    public KRow addProperty(
        final String property,
        final Object o
    ) {
        this.addColumn(property, o);
        
        return this;
    }
    
    public KRow cloneMe() {
        return new KRow((this.o != null) ? this.o.clone() : null, (this.ref != null ? new HashMap(this.ref) : null), this.isNull);
    }
    
    protected <T> T getByClass(
        final String column,
        final Class<T> clazz
    ) {
        if (clazz == Object.class) {
            return (T) this.get(column);
        }
        
        if (clazz == String.class) {
            return (T) this.getString(column);
        }
        
        if (clazz == Character.class) {
            return (T) this.getCharacter(column);
        }
        
        if (clazz == UUID.class) {
            return (T) this.getUUID(column);
        }
        
        if (clazz == BigDecimal.class) {
            return (T) this.getBigDecimal(column);
        }
        
        if (clazz == BigInteger.class) {
            return (T) this.getBigInteger(column);
        }
        
        if (clazz == Long.class) {
            return (T) this.getLong(column);
        }
        
        if (clazz == Integer.class) {
            return (T) this.getInteger(column);
        }
        
        if (clazz == Boolean.class) {
            return (T) this.getBoolean(column);
        }
        
        if (clazz == Double.class) {
            return (T) this.getDouble(column);
        }
        
        if (clazz == LocalDateTime.class) {
            return (T) this.getLocalDateTime(column);
        }
        
        if (clazz == LocalDate.class) {
            return (T) this.getLocalDate(column);
        }
        
        if (clazz == Date.class) {
            return (T) this.getDate(column);
        }
        
        if (clazz == Timestamp.class) {
            return (T) this.getTimestamp(column);
        }
        
        throw KExceptionHelper.internalServerError("Class not supported.");
    }
    
    private String throwNoCasteableMessage(
        final int n,
        final String nameRef,
        final String type
    ) {
        if (nameRef != null) {
            throw KExceptionHelper.internalServerError("'" + nameRef + "' value is not casteable to \"" + type + "\"");
        }

        throw KExceptionHelper.internalServerError("Value at position '" + n + "' is not casteable to \"" + type + "\"");
    }
}
