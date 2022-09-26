package com.newsbreak.data.udf;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created on 2021/5/18.
 *
 * @author wei.liu
 */
public class ShuffleIntoBucketUDFTest {

    @Test
    public void testBucket1() {
        ShuffleIntoBucketUDF udf = new ShuffleIntoBucketUDF();
        udf.initABFManager(null);
        udf.process(Collections.singletonList("1111").toArray());
    }

    @Test
    public void testBucket2() {
        ShuffleIntoBucketUDF udf = new ShuffleIntoBucketUDF();
        udf.initABFManager(Arrays.asList("feed,push".split(",")));
        udf.process(Collections.singletonList("1111").toArray());
    }

}