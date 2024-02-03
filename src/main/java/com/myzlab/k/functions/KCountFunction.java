package com.myzlab.k.functions;

import com.myzlab.k.KQuery;

@FunctionalInterface
public interface KCountFunction<KFrom, T extends KQuery> {
    T run(KFrom kFrom);
}
