package com.xad.pig.udf.hll;

import net.agkn.hll.HLLType;

/**
 * author: karthik
 * date: 4/24/15
 */
public class HLLConfig {
    private static final int DEFAULT_LOG2M = 10;
    private static final int DEFAULT_REGWIDTH = 5;
    private static final int DEFAULT_EXPTHRESH = 11;
    private static final boolean DEFAULT_SPARESON = true;
    private static final HLLType DEFAULT_TYPE = HLLType.EMPTY;

    public HLLType getType() {
        return type;
    }

    public int getLog2m() {
        return log2m;
    }

    public int getRegwidth() {
        return regwidth;
    }

    public int getExpthresh() {
        return expthresh;
    }

    public boolean isSparseon() {
        return sparseon;
    }

    private int log2m = DEFAULT_LOG2M;
    private int regwidth = DEFAULT_REGWIDTH;
    private int expthresh = DEFAULT_EXPTHRESH;
    private boolean sparseon = DEFAULT_SPARESON;
    private HLLType type = DEFAULT_TYPE;

    public HLLConfig(){}
    public HLLConfig(int log2m, int regwidth, int expthresh, boolean sparseon){
        this.log2m = log2m;
        this.regwidth = regwidth;
        this.expthresh = expthresh;
        this.sparseon = sparseon;
    }
}
