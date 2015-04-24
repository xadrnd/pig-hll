package com.xad.pig.udf.hll;

/**
 * author: karthik
 * date: 2/23/15
 */
import net.agkn.hll.util.NumberUtil;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_CREATE extends HyperLogLogUdfBase<String> {


    // Default constructor
    public HLL_CREATE() {
        super();
    }

    public HLL_CREATE(String[] args) {
        super(args);
    }

    public HLL_CREATE(int log2m, int regwidth, int expthresh, boolean sparseon) {
        super(log2m, regwidth, expthresh, sparseon);
    }


    public String getInitial() {return InitialScalar.class.getName();}
    public String getFinal() {return FinalHll.class.getName();}

    @Override
    public String exec(Tuple tuple) throws IOException {
        byte[] bytes = hllFromValues(tuple, hllConfig).toBytes();
        return NumberUtil.toHex(bytes, 0, bytes.length);
    }
}