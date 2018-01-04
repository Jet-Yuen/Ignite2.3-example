package com.bsfit.data.service.job;

import org.apache.ignite.cache.query.annotations.QuerySqlFunction;

/**
 * @author Jet
 */
public class IgniteFunction {

    private IgniteFunction() {
    }

    @QuerySqlFunction
    public static int matchFun(String province, String contains) {
        if (province.contains(contains)) {
            return 1;
        }
        return 0;
    }

}
