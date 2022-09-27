package com.newsbreak.data.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created on 2021/5/6.
 *
 * @author wei.liu
 */

@UDFType(deterministic = false)
@Description(name = "parse_bucket_dist",
        value = "_FUNC_(user_id) - Shuffle Into one of 1000 Buckets each layer By user_id, "
        + " you can limit layers by arg2 in the format, for example: feed,push")
public class ParseBucketDistUDF extends GenericUDTF {

    public StructObjectInspector outputField(List<String> colNames) {
        List<ObjectInspector> colTypes = colNames.stream()
                .map(l -> PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                .collect(Collectors.toList());
        return ObjectInspectorFactory.getStandardStructObjectInspector(colNames, colTypes);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        System.out.println("----> udf initialize start |");

        if (argOIs.length == 1) {
            if (null == argOIs[0] || argOIs[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                throw new UDFArgumentException("udf need string parameter");
            }
        } else {
            throw new UDFArgumentLengthException("udf takes only one arguments, actually is : " + argOIs.length);
        }

        return outputField(Arrays.asList("bucket_id", "exp_name", "ver_name"));
    }

    @Override
    public void process(Object[] args) {
        String bucketDistJsonStr = args[0].toString();
        JSONObject bucketDist = new JSONObject(bucketDistJsonStr);

        Iterator<String> iterator = bucketDist.keys();
        while(iterator.hasNext()){

            String key = iterator.next();
            List<String> expVer = Arrays.asList(bucketDist.getString(key).split("@"));

            String expName  = expVer.get(0);
            String verName  = expVer.get(1);
            String bucketId = String.format("%03d", Integer.parseInt(key));

            try {
                this.forward(new String[]{bucketId, expName, verName});
            } catch (Exception e) {
                System.out.println("***********本机调试***********");
                System.out.println(bucketId + " " + expName + " " + verName );
            }

        }



    }

    @Override
    public void close() throws HiveException {
        // nothing to do for me
    }

}
