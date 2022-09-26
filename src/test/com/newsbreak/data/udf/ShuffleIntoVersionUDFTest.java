package com.newsbreak.data.udf;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created on 2021/5/18.
 *
 * @author wei.liu
 */
public class ShuffleIntoVersionUDFTest {

    @Test
    public void testVersion1() {
        ShuffleIntoVersionUDF udf = new ShuffleIntoVersionUDF();
        udf.initABFManager(null);
        udf.process(Collections.singletonList("1111").toArray());
    }

    @Test
    public void testVersion2() {
        ShuffleIntoVersionUDF udf = new ShuffleIntoVersionUDF();
        udf.initABFManager(Arrays.asList("foryou,push".split(",")));
        udf.process(Collections.singletonList("1111").toArray());
    }

    @Test
    public void testVersion3() {
        ShuffleIntoVersionUDF udf = new ShuffleIntoVersionUDF();
        udf.initABFManager(Arrays.asList("foryou".split(",")));
        udf.process(Arrays.asList("1111", "{\"platform\" :\"ios\"}").toArray());
    }

}