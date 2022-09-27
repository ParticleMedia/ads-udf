package com.newsbreak.data.udf;

import com.nb.data.ab.bean.ABContext;
import com.newsbreak.data.utils.ABFManager;
import org.apache.commons.lang3.StringUtils;
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
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 2022/9/20.
 *
 * @author wei.liu
 */

@UDFType(deterministic = false)
@Description(name = "shuffle_into_bucket",
        value = "_FUNC_(user_id) - Shuffle Into one of 1000 Buckets each layer By user_id, "
        + " you can limit layers by arg2 in the format, for example: feed,push")
public class ShuffleIntoBucketUDF extends GenericUDTF {

    protected void initABFManager(List<String> layerNames){
        //  AB Initialize this GenericUDTF. This will be called only once per instance.
        try {
            ABFManager.init(ABFManager.Env.PROD, layerNames);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void initialize1Arg(ObjectInspector arg1) throws UDFArgumentException {
        if (null == arg1 || arg1.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("udf need string parameter");
        }
    }

    protected StructObjectInspector outputField(List<String> colNames) {
        List<ObjectInspector> colTypes = colNames.stream()
                .map(l -> PrimitiveObjectInspectorFactory.javaStringObjectInspector)
                .collect(Collectors.toList());
        return ObjectInspectorFactory.getStandardStructObjectInspector(colNames, colTypes);
    }

    protected void addCondition(String k, ABContext.Builder builder, JSONObject fromJsonObject) {
        if (fromJsonObject.has(k) && StringUtils.isNotBlank(fromJsonObject.getString(k))) {
            builder.addCondition(k, fromJsonObject.getString(k).trim());
        }
    }

    protected ABContext buildABContext(Object[] args) {

        String user_id = args[0].toString();

        ABContext.Builder builder = ABContext.create().withFactor(user_id);

        if (2 == args.length && null != args[1] && StringUtils.isNotBlank(args[1].toString())) {
            try {
                JSONObject ctx = new JSONObject(args[1].toString().trim());

                List<String> CTX_KEYS = Arrays.asList("platform", "cv", "uid");
                for (String k: CTX_KEYS) {
                    addCondition(k, builder, ctx);
                }
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        return builder.build();

    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        System.out.println("----> udf initialize start |");

        if (argOIs.length == 1) {
            initialize1Arg(argOIs[0]);
        } else {
            throw new UDFArgumentLengthException("udf takes only one arguments, actually is : " + argOIs.length);
        }

        initABFManager(null);

        return outputField(Arrays.asList("layer_name", "bucket_id"));
    }

    @Override
    public void process(Object[] args) {
        ABContext abContext = buildABContext(args);

        Map<String, String> buckets = ABFManager.abBucketRemote(abContext);

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
    public void close() throws HiveException {
        // nothing to do for me
    }

}
