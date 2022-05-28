package com.newsbreak.data.udf;

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
@Description(name = "abv3_versions",
        value = "_FUNC_(user_id) - Generates a range of integers from a to b incremented by c"
        + " or the elements of a map into multiple rows and columns ")
public class NBABV3UDF extends GenericUDTF {

    public NBABV3UDF() throws Exception {
        ABFManager.init(ABFManager.Env.PROD, null);
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        ObjectInspector[] args = new ObjectInspector[inputFields.size()];

        if (args.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }

        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }

        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("layer_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("bucket_num");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) {

        String user_id = args[0].toString();

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
                continue;
            }
        }
    }

    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub
    }

    /*
    public static void main(String[] args) throws HiveException {
        NBABV3UDF nbabv3UDF = new NBABV3UDF();

        Object[] arg0 = new Object[2];
        arg0[0]="1111";
        nbabv3UDF.process(arg0);
    }

     */

}
