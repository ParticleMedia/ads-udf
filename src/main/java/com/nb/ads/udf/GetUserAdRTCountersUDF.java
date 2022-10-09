package com.nb.ads.udf;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import com.nb.ads.utils.UserAdRTCountersProcessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

public class GetUserAdRTCountersUDF extends GenericUDF {
  private transient ListObjectInspector outOI;
  private transient ListObjectInspector innerOI;

  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[3];
  private transient ObjectInspectorConverters.Converter[] converters = new Converter[3];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 4, 4);
    for (int i = 0; i < 3; ++i) {
      checkArgPrimitive(arguments, i);
      checkArgGroups(arguments, i, inputTypes, STRING_GROUP);
      obtainStringConverter(arguments, i, inputTypes, converters);
    }

    assert (arguments[3] instanceof ListObjectInspector);
    outOI = (ListObjectInspector) arguments[3];
    assert (outOI.getListElementObjectInspector() instanceof ListObjectInspector);
    innerOI = (ListObjectInspector) outOI.getListElementObjectInspector();
    assert (innerOI.getListElementObjectInspector() instanceof StringObjectInspector);

    // return Map<String, Double>
    GenericUDFUtils.ReturnObjectInspectorResolver keyOIResolver =
        new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    GenericUDFUtils.ReturnObjectInspectorResolver valueOIResolver =
        new GenericUDFUtils.ReturnObjectInspectorResolver(true);
    ObjectInspector keyOI =
        keyOIResolver.get(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    ObjectInspector valueOI =
        valueOIResolver.get(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    return ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    // better sanity check
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    String userFeature = getStringValue(arguments, 0, converters);
    String adFeature = getStringValue(arguments, 1, converters);
    Long adId = Long.parseLong(getStringValue(arguments, 2, converters));
    Map<Long, List<Long>> adInfos = new HashMap<>();
    List<Object> outer = (List<Object>) outOI.getList(arguments[3].get());
    for (int i = 0; i < outer.size(); ++i) {
      List<Text> inner = (List<Text>) innerOI.getList(outer.get(i));
      List<Long> adInfo = new ArrayList<>();
      for (Text attr : inner) {
        try {
          adInfo.add(Long.parseLong(attr.toString()));
        } catch (NumberFormatException e) {
          adInfo.add(-1L);
        }
      }
      adInfos.put(adInfo.get(0), adInfo);
    }

    return UserAdRTCountersProcessor.computeCounters(userFeature, adFeature, adInfos, adId);
  }

  @Override
  public String getDisplayString(String[] children) {
    // assert (children.length == 3);
    return getStandardDisplayString("GetUserAdRTCountersUDF", children);
  }
}
