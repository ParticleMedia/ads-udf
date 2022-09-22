package com.newsbreak.data.udf;

import avro.shaded.com.google.common.collect.Lists;
import com.nb.data.ab.bean.ABContext;
import com.newsbreak.data.utils.ABFManager;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created on 2021/5/6.
 *
 * @author wei.liu
 */

@UDFType(deterministic = false)
@Description(name = "nb_abv3",
        value = "_FUNC_(user_id) - Generates a range of integers from a to b incremented by c"
        + " or the elements of a map into multiple rows and columns ")
public class NBABV3UDF extends GenericUDTF {

    private List<String> colName = Lists.newLinkedList();
    private List<ObjectInspector> resType = Lists.newLinkedList();

    public NBABV3UDF() {
        // ABFManager.init(ABFManager.Env.PROD, null);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {

        if (argOIs.length != 1) {
            throw new UDFArgumentLengthException("NBABV3UDF takes only one argument");
        }

        if (argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("NBABV3UDF takes string as a parameter");
        }

        colName.add("layer_name");
        colName.add("bucket_num");
        resType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        resType.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        // 返回分别为列名 和 列类型
        return ObjectInspectorFactory.getStandardStructObjectInspector(colName, resType);

    }

    @Override
    public void process(Object[] args) {

        String user_id = args[0].toString();

        try {
            ABFManager.init(ABFManager.Env.PROD, null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ABContext ctx = ABContext.create()
                .withFactor(user_id)  // most time, factor use userid, anything you like
                // .addCondition("platform", "android")  // everything want to use in conditions for filter user
                // .addCondition("cv", "070202")
                // .addCondition("uid", 29046205)  // ! uid set is needed, won't use factor for condition
                .build();

        Map<String, String> buckets = ABFManager.abBucket(ctx);

        for (Map.Entry<String, String> entry : buckets.entrySet()) {
            try {
                this.forward(new String[]{entry.getKey(), entry.getValue()});
            } catch (Exception e) {
                System.out.println("***********本机调试***********");
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub
    }

}
