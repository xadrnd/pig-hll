package com.xad.pig.udf.hll;

/**
 * author: karthik
 * date: 2/23/15
 */
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_MERGE_COMPUTE extends HyperLogLogUdfBase<Long> {
    // Default constructor
    public HLL_MERGE_COMPUTE() {
        super();
    }

    public HLL_MERGE_COMPUTE(String[] args) {
        super(args);
    }

    public HLL_MERGE_COMPUTE(int log2m, int regwidth, int expthresh, boolean sparseon) {
        super(log2m, regwidth, expthresh, sparseon);
    }

    public String getInitial() {return InitialHLL.class.getName();}
    public String getFinal() {return FinalEstimate.class.getName();}

    @Override
    public Long exec(Tuple tuple) throws IOException {
        return hllFromHlls(tuple).cardinality();
    }

}
