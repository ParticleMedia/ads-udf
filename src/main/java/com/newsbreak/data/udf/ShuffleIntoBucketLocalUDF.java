package com.newsbreak.data.udf;

import com.newsbreak.data.utils.ABFService;
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

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created on 2021/5/6.
 *
 * @author wei.liu
 */

@UDFType(deterministic = false)
@Description(name = "shuffle_into_bucket_local",
        value = "_FUNC_(user_id) - Shuffle Into one of 1000 Buckets each layer By user_id, "
        + " you can limit layers by arg2 in the format, for example: feed,push")
public class ShuffleIntoBucketLocalUDF extends GenericUDTF {

    static public String getBucketIdLocal(String factor, String layerName, String shuffleTs) {
        String shufflePrefix = MessageFormat.format("{0}{1}", layerName, Optional.ofNullable(shuffleTs).orElse(""));
        return ABFService.getBucketIdLocal(factor, shufflePrefix);
    }


    public void initialize1Arg(ObjectInspector arg1) throws UDFArgumentException {
        if (null == arg1 || arg1.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("udf need string parameter");
        }
    }

    public StructObjectInspector outputField(List<String> colNames) {
        List<ObjectInspector> colTypes = colNames.stream()
                .map(l -> PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                .collect(Collectors.toList());
        return ObjectInspectorFactory.getStandardStructObjectInspector(colNames, colTypes);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        System.out.println("----> udf initialize start |");

        if (argOIs.length == 3) {
            initialize1Arg(argOIs[0]);
            initialize1Arg(argOIs[1]);
            initialize1Arg(argOIs[2]);
        } else {
            throw new UDFArgumentLengthException("udf takes only one arguments, actually is : " + argOIs.length);
        }

        return outputField(Arrays.asList("layer_name", "bucket_id"));
    }

    @Override
    public void process(Object[] args) {
        String userId = args[0].toString();
        String layerName = args[1].toString();
        String shuffleTs = args[2].toString();

        String bucketIdLocal = getBucketIdLocal(userId, layerName, shuffleTs);

        try {
            this.forward(new String[]{layerName, bucketIdLocal});
        } catch (Exception e) {
            System.out.println("***********本机调试***********");
            System.out.println(layerName + " " + bucketIdLocal);
        }
    }

    @Override
    public void close() throws HiveException {
        // nothing to do for me
    }

}
