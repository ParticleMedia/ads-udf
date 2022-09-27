package com.newsbreak.data.udf;

import com.nb.data.ab.bean.ABContext;
import com.newsbreak.data.utils.ABFManager;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.Arrays;
import java.util.Map;

/**
 * Created on 2022/9/20.
 *
 * @author wei.liu
 */

@UDFType(deterministic = false)
@Description(name = "shuffle_into_version",
        value = "_FUNC_(user_id) - Shuffle Into exp&ver By user_id, ignore conditions of experiment "
                + " you can limit layers by arg2 in the format, for example: feed,push")
public class ShuffleIntoVersionUDF extends ShuffleIntoBucketUDF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        System.out.println("----> udf initialize start |");

        if (argOIs.length == 1) {
            initialize1Arg(argOIs[0]);
        } else if (argOIs.length == 2) {
            initialize1Arg(argOIs[0]);
            initialize1Arg(argOIs[1]);
        } else {
            throw new UDFArgumentLengthException("udf takes only one or two arguments, actually is : " + argOIs.length);
        }

        initABFManager(null);

        return outputField(Arrays.asList("exp_name", "ver_name"));
    }

    @Override
    public void process(Object[] args) {
        ABContext abContext = buildABContext(args);

        Map<String, String> buckets = ABFManager.abVersionRemote(abContext);

        for (Map.Entry<String, String> entry : buckets.entrySet()) {
            try {
                this.forward(new String[]{entry.getKey(), entry.getValue()});
            } catch (Exception e) {
                System.out.println("***********本机调试***********");
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        }
    }

}
