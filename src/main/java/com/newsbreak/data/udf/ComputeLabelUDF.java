package com.newsbreak.data.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * ComputeLabelUDF
 * @author Xuetao
 */

@Description(name = "ComputeLabelUDF", value = "ComputeLabelUDF(checked, clicked, autoplay, shared, liked, thumbed_up, thumbed_down, pv_time, cv_time, vv_time, ctype, progress)",
        extended = "")
public class ComputeLabelUDF extends GenericUDF {
    private ObjectInspectorConverters.Converter[] converters;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 12) {
            throw new UDFArgumentLengthException(
                    "ComputeLabelUDF(checked, clicked, autoplay, shared, liked, thumbed_up, thumbed_down, pv_time, cv_time, vv_time, ctype, progress)");
        }

        converters = new ObjectInspectorConverters.Converter[arguments.length];

        // tinyint
        for (int i = 0; i < 7; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableShortObjectInspector);
        }

        // int features
        for (int i = 7; i < 10; i++) {
            converters[i] = ObjectInspectorConverters.getConverter(arguments[i], PrimitiveObjectInspectorFactory.writableIntObjectInspector);
        }

        converters[10] = ObjectInspectorConverters.getConverter(arguments[10],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        converters[11] = ObjectInspectorConverters.getConverter(arguments[11],
                PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 12);

        short checked = GetShortValue(0, arguments);
        short clicked = GetShortValue(1, arguments);
        short autoplay = GetShortValue(2, arguments);
        short shared = GetShortValue(3, arguments);
        short liked = GetShortValue(4, arguments);
        short thumbed_up = GetShortValue(5, arguments);
        short thumbed_down = GetShortValue(6, arguments);

        int pv_time = GetIntValue(7, arguments);
        int cv_time = GetIntValue(8, arguments);
        int vv_time = GetIntValue(9, arguments);

        Text ctype = (Text) converters[10].convert(arguments[10].get());
        float progress = GetFloatValue(11, arguments);

        return AssignLabel(ctype, checked, clicked, autoplay, shared, liked, thumbed_up, thumbed_down, pv_time, cv_time, vv_time, progress);
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 12);
        return "ComputeLabelUDF(*)";
    }

    private short GetShortValue(int index, DeferredObject[] arguments) throws HiveException {
        ShortWritable temp = (ShortWritable) converters[index].convert(arguments[index].get());
        return (temp == null) ? 0 : temp.get();
    }

    private float GetFloatValue(int index, DeferredObject[] arguments) throws HiveException {
        FloatWritable temp = (FloatWritable) converters[index].convert(arguments[index].get());
        return (temp == null) ? 0.0f : temp.get();
    }

    private int GetIntValue(int index, DeferredObject[] arguments) throws HiveException {
        IntWritable temp = (IntWritable) converters[index].convert(arguments[index].get());
        return (temp == null) ? 0 : temp.get();
    }

    private Boolean AssignLabel(Text ctype, Short checked, Short clicked, Short autoplay, Short shared, Short liked, Short thumbed_up, Short thumbed_down, Integer pv_time, Integer cv_time, Integer vv_time, Float progress)
    {
        if (shared == 1 || thumbed_up == 1 || liked == 1)
        {
            return true;
        }

        if (clicked == 1) {
            if ("video_web".equals(ctype)) {
                return vv_time > 10000 || progress > 0.2;
            }
            else {
                return pv_time > 1999;
            }
        }

        if (autoplay == 1) {
            return vv_time >= 20000;
        }

        return false;
    }
}