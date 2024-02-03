package com.myzlab.k.functions;

import com.myzlab.k.KQuery;

@FunctionalInterface
public interface KAssertExistsFunction<KWhere, T extends KQuery> {
    T run(KWhere kWhere);
}
