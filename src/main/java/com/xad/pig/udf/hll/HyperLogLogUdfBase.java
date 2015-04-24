package com.xad.pig.udf.hll;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import net.agkn.hll.HLL;
import net.agkn.hll.util.NumberUtil;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
* author: karthik
* date: 2/23/15
*/

public abstract class HyperLogLogUdfBase<TReturnType>
        extends EvalFunc<TReturnType> implements Algebraic {

    protected static final String SCALAR_VALUE = "ScalarValue";
    protected static final String HLL_VALUE = "HLL";
    static final int seed = 123456;
    static final HashFunction hash = Hashing.murmur3_128(seed);
    static final Hasher hasher = hash.newHasher();


    protected HLLConfig hllConfig;

    // Default constructor
    public HyperLogLogUdfBase() { this.hllConfig = new HLLConfig();}

    public HyperLogLogUdfBase(Integer log2m, Integer regwidth, Integer expthresh, boolean sparseon) {
        this.hllConfig = new HLLConfig(log2m, regwidth, expthresh, sparseon);
    }

    public HyperLogLogUdfBase(String[] args) {
        this(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]), Boolean.parseBoolean(args[3]));
    }

    static public class InitialScalar extends EvalFunc<Tuple> {
        public InitialScalar(){}
        public InitialScalar(Integer log2m, Integer regwidth, Integer expthresh, boolean sparseon) {}
        public InitialScalar(String[] args) {
            this(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]), Boolean.parseBoolean(args[3]));
        }

        public Tuple exec(Tuple input) throws IOException {
            return tupleFromSingleValue(input, SCALAR_VALUE);
        }
    }

    static public class InitialHLL extends EvalFunc<Tuple> {
        public InitialHLL(){}
        public InitialHLL(Integer log2m, Integer regwidth, Integer expthresh, boolean sparseon) {}
        public InitialHLL(String[] args) {
            this(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]), Boolean.parseBoolean(args[3]));
        }
        public Tuple exec(Tuple input) throws IOException {
            return tupleFromSingleValue(input, HLL_VALUE);
        }
    }

    public String getIntermed() {return Intermed.class.getName();}

    static public class Intermed extends EvalFunc<Tuple> {
        HLLConfig hllConfig;
        // Default constructor
        public Intermed() { this.hllConfig = new HLLConfig();}

        public Intermed(Integer log2m, Integer regwidth, Integer expthresh, boolean sparseon) {
            this.hllConfig = new HLLConfig(log2m, regwidth, expthresh, sparseon);
        }

        public Intermed(String[] args) {
            this(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]), Boolean.parseBoolean(args[3]));
        }
        public Tuple exec(Tuple input) throws IOException {
            byte[] bytes = hllFromTuples(input, hllConfig).toBytes();
            return TupleFactory.getInstance().newTuple(Arrays.asList(HLL_VALUE, NumberUtil.toHex(bytes, 0, bytes.length)));
        }

    }


    static public class FinalEstimate extends EvalFunc<Long> {
        HLLConfig hllConfig;
        // Default constructor
        public FinalEstimate() { this.hllConfig = new HLLConfig();}

        public FinalEstimate(Integer log2m, Integer regwidth, Integer expthresh, boolean sparseon) {
            this.hllConfig = new HLLConfig(log2m, regwidth, expthresh, sparseon);
        }

        public FinalEstimate(String[] args) {
            this(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]), Boolean.parseBoolean(args[3]));
        }
        public Long exec(Tuple input) throws IOException {
            return (long) hllFromTuples(input, hllConfig).cardinality();
        }
    }

    static public class FinalHll extends EvalFunc<String> {
        HLLConfig hllConfig;
        // Default constructor
        public FinalHll() { this.hllConfig = new HLLConfig();}

        public FinalHll(Integer log2m, Integer regwidth, Integer expthresh, boolean sparseon) {
            this.hllConfig = new HLLConfig(log2m, regwidth, expthresh, sparseon);
        }

        public FinalHll(String[] args) {
            this(Integer.valueOf(args[0]), Integer.valueOf(args[1]), Integer.valueOf(args[2]), Boolean.parseBoolean(args[3]));
        }
        public String exec(Tuple input) throws IOException {
            byte[] bytes = hllFromTuples(input, hllConfig).toBytes();
            return NumberUtil.toHex(bytes, 0, bytes.length);
        }
    }

    private static interface InputAction {
        HLL call(HLL current, String item);
    }

    private static HLL iterateInput(Tuple input, InputAction action) throws ExecException {
        Object values = input.get(0);

        if (values instanceof String) {
            return action.call(null, values.toString());
        }

        if (values instanceof DataBag) {
            DataBag data = (DataBag) values;

            HLL current = null;
            for (Tuple value : data) {
                current = action.call(current, value.get(0).toString());
            }
            return current;
        }

        return null;
    }

    protected static HLL hllFromTuples(Tuple input, HLLConfig hllConfig) throws ExecException {
        HLL hll = null;
        Object values = input.get(0);

        if (values instanceof DataBag) {
            DataBag data = (DataBag)values;

            for (Tuple value : data) {
                if (value.size() == 0) {
                    continue;
                }

                String valueType = value.get(0).toString();
                String valueStr = value.get(1).toString();
                if (valueType.equals(SCALAR_VALUE)) {
                    if (hll == null)
                        hll =  new HLL(hllConfig.getLog2m(), hllConfig.getRegwidth(), hllConfig.getExpthresh(), hllConfig.isSparseon(), hllConfig.getType());
                    hll.addRaw(hash.hashString(valueStr, Charset.defaultCharset()).asLong());

                } else {
                    HLL currentHll = HLL.fromBytes(NumberUtil.fromHex(valueStr, 0, valueStr.length()));
                    if (hll == null) {
                        hll = currentHll;
                    } else {
                        hll.union(currentHll);
                    }
                }
            }
        }

        return hll;
    }

    protected static HLL hllFromHlls(Tuple input) throws ExecException {
        return iterateInput(input, new InputAction() {
            public HLL call(HLL current, String item) {
                HLL newHll = HLL.fromBytes(NumberUtil.fromHex(item, 0, item.length()));
                if (current != null) {
                    current.union(newHll);
                    return current;
                } else {
                    return newHll;
                }
            }
        });
    }


    protected static HLL hllFromValues(Tuple input, final HLLConfig hllConfig) throws ExecException {
        return iterateInput(input, new InputAction() {
            public HLL call(HLL current, String item) {
                if (current == null)
                    current = new HLL(hllConfig.getLog2m(), hllConfig.getRegwidth(), hllConfig.getExpthresh(), hllConfig.isSparseon(), hllConfig.getType());
                current.addRaw(hash.hashString(item, Charset.defaultCharset()).asLong());
                return current;
            }
        });
    }

    protected static Tuple tupleFromSingleValue(Tuple input, String valueType) throws ExecException {
        DataBag data = (DataBag) input.get(0);

        if (data.size() == 0) {
            return TupleFactory.getInstance().newTuple();
        }

        Tuple value = data.iterator().next();
        Object fieldValue = value.get(0);
        return TupleFactory.getInstance().newTuple(Arrays.asList(valueType, fieldValue == null ? "" : fieldValue.toString()));
    }
}