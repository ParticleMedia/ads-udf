package com.newsbreak.data.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

/**
 * Created on 2021/5/18.
 *
 * @author wei.liu
 */
public class NBABV3UDFTest {

    // @Test
    public void initialize() throws Exception {
        NBABV3UDF nbabv3UDF = new NBABV3UDF();
        System.out.print(nbabv3UDF);

        Object[] arg0 = new Object[2];
        arg0[0]="1111";
        nbabv3UDF.process(arg0);
    }

    @Test
    public void process() throws Exception {
        System.out.print("processing ...");
        initialize();
    }
}