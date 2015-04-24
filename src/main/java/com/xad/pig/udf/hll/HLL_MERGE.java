package com.xad.pig.udf.hll;

/**
 * author: karthik
 * date: 2/23/15
 */

import net.agkn.hll.util.NumberUtil;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_MERGE extends HyperLogLogUdfBase<String> {
    // Default constructor
    public HLL_MERGE() {
        super();
    }

    public HLL_MERGE(String[] args) {
        super(args);
    }

    public HLL_MERGE(int log2m, int regwidth, int expthresh, boolean sparseon) {
        super(log2m, regwidth, expthresh, sparseon);
    }
    public String getInitial() {return InitialHLL.class.getName();}
    public String getFinal() {return FinalHll.class.getName();}

    @Override
    public String exec(Tuple tuple) throws IOException {
        byte[] bytes = hllFromHlls(tuple).toBytes();
        return NumberUtil.toHex(bytes, 0, bytes.length);
    }

}