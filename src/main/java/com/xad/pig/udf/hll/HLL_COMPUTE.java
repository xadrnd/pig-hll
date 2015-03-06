package com.xad.pig.udf.hll;

/**
 * author: karthik
 * date: 2/23/15
 */
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_COMPUTE extends HyperLogLogUdfBase<Long> {

    public String getInitial() {return InitialScalar.class.getName();}
    public String getFinal() {return FinalEstimate.class.getName();}

    @Override
    public Long exec(Tuple tuple) throws IOException {
        return hllFromValues(tuple, normalHll()).cardinality();
    }
}
