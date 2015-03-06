package com.xad.pig.udf.hll;

/**
 * author: karthik
 * date: 2/23/15
 */
import net.agkn.hll.util.NumberUtil;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class HLL_CREATE extends HyperLogLogUdfBase<String> {

    public String getInitial() {return InitialScalar.class.getName();}
    public String getFinal() {return FinalHll.class.getName();}

    @Override
    public String exec(Tuple tuple) throws IOException {
        byte[] bytes = hllFromValues(tuple, normalHll()).toBytes();
        return NumberUtil.toHex(bytes, 0, bytes.length);
    }
}