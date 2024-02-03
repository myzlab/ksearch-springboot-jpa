package com.myzlab.k.functions;

import com.myzlab.k.KQuery;

@FunctionalInterface
public interface KFindMultipleFunction<KFrom, T extends KQuery> {
    T run(KFrom kFrom);
}
