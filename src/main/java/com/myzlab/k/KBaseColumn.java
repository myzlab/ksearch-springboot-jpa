package com.myzlab.k;

import com.myzlab.k.helper.KExceptionHelper;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public abstract class KBaseColumn {

    protected final StringBuilder sb = new StringBuilder();
    protected final List<Object> params = new ArrayList<>();
    protected boolean closed = true;
    protected final String name;
    protected final Class type;
    protected final KTable kTable;
    protected final KColumnDataType columnDataType;
    
    protected KBaseColumn() {
        super();
        
        this.name = null;
        this.type = null;
        this.kTable = null;
        this.columnDataType = null;
    }
    
    protected KBaseColumn(
        final boolean closed
    ) {
        super();
        
        this.closed = closed;
        this.name = null;
        this.type = null;
        this.kTable = null;
        this.columnDataType = null;
    }
    
    protected KBaseColumn(
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        super();
        
        this.closed = closed;
        this.name = name;
        this.type = type;
        this.kTable = kTable;
        this.columnDataType = null;
    }
    
    protected KBaseColumn(
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable,
        final KColumnDataType columnDataType
    ) {
        super();
        
        this.closed = closed;
        this.name = name;
        this.type = type;
        this.kTable = kTable;
        this.columnDataType = columnDataType;
    }
    
    protected KBaseColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed
    ) {
        this(closed);
        
        this.sb.append(sb);
        this.params.addAll(params);
    }
    
    protected KBaseColumn(
        final StringBuilder sb,
        final List<Object> params,
        final boolean closed,
        final String name,
        final Class type,
        final KTable kTable
    ) {
        this(closed, name, type, kTable);
        
        this.sb.append(sb);
        this.params.addAll(params);
    }
    
    protected KBaseColumn(
        final StringBuilder sb
    ) {
        this();
        
        this.sb.append(sb);
    }
    
    protected KBaseColumn(
        final String val
    ) {
        this();
        
        if (val == null) {
            throw KExceptionHelper.internalServerError("The 'val' param cannot be null");
        }
        
        this.sb.append("?");
        this.params.add(val);
    }
    
    protected KBaseColumn(
        final Number val
    ) {
        this();
        
        if (val == null) {
            throw KExceptionHelper.internalServerError("The 'val' param cannot be null");
        }
        
        this.sb.append("?");
        this.params.add(val);
    }
    
    protected KBaseColumn(
        final UUID val
    ) {
        this();
        
        if (val == null) {
            throw KExceptionHelper.internalServerError("The 'val' param cannot be null");
        }
        
        this.sb.append("?");
        this.params.add(val);
    }
    
    protected KBaseColumn(
        final LocalDate val
    ) {
        this();
        
        if (val == null) {
            throw KExceptionHelper.internalServerError("The 'val' param cannot be null");
        }
        
        this.sb.append("?");
        this.params.add(val);
    }
    
    protected KBaseColumn(
        final LocalDateTime val
    ) {
        this();
        
        if (val == null) {
            throw KExceptionHelper.internalServerError("The 'val' param cannot be null");
        }
        
        this.sb.append("?");
        this.params.add(val);
    }

    @Override
    public String toString() {
        if (this.kTable != null && this.kTable.alias != null) {
            return this.kTable.alias + "." + this.name;
        }
        
        return this.name;
    }
    
    protected abstract KBaseColumn cloneMe();
}
