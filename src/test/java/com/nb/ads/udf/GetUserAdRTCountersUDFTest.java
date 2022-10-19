package com.nb.ads.udf;

import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetUserAdRTCountersUDFTest {
  private static final Double DELTA = 0.00001;
  private static final int FULL_FEATURE_COUNT = 3 + 3 * 5 + 3 + 3 * 5;
  Map<String, Double> oi;
  private Gson gson;
  private ObjectInspector[] inputOIs;

  @Before
  public void setUp() throws Exception {
    List<ObjectInspector> tuple = new ArrayList<ObjectInspector>();
    for (int i = 0; i < 3; ++i) {
      tuple.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }
    tuple.add(
        ObjectInspectorFactory.getStandardListObjectInspector(
            ObjectInspectorFactory.getStandardListObjectInspector(
                PrimitiveObjectInspectorFactory.writableStringObjectInspector)));

    inputOIs = new ObjectInspector[tuple.size()];
    inputOIs = tuple.toArray(inputOIs);
    gson = new Gson();
  }

  @After
  public void tearDown() throws Exception {
    System.out.println(oi.toString());
  }

  @Test
  public void completeFeatureUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION",
        "[[1577702671705346050,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1577702671705346050,3]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CONVERSION", "[[1577702671705346050,5]]");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_conversion_24hr", "[2.0]");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1577702671705346050");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(
                new Text("1577702671705346050"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNotNull(oi.get("user_ctr_24hr"));
    Assert.assertEquals(0.2, oi.get("user_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT, oi.size());
  }

  @Test
  public void userFeaturePartEmpytUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION", "[[1111,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1111,3]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CONVERSION", "[]");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_conversion_24hr", "[2.0]");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1111");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(new Text("1111"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNull(oi.get("user_cvr_24hr"));
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT - 5 - 5 * 2, oi.size());
    System.out.println(oi.toString());
  }

  @Test
  public void featurePartEmptyUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION", "[[1111,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1111,3]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CONVERSION", "[]");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_conversion_24hr", "[]");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1111");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(new Text("1111"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNull(oi.get("user_cvr_24hr"));
    Assert.assertNull(oi.get("ad_cvr_24hr"));
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT - 5 - 5 * 2 - 2 - 1, oi.size());
  }

  @Test
  public void featurePartMissingUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION", "[[1111,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1111,3]]");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1111");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(new Text("1111"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNull(oi.get("user_cvr_24hr"));
    Assert.assertNull(oi.get("ad_cvr_24hr"));
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT - 5 - 5 * 2 - 2 - 1, oi.size());
  }

  @Test
  public void featureKeyWrongUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION", "[[1111,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1111,3]]");
    userFeatureMap.put("WRONG_KEY", "[]");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("wrong_key", "[]");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1111");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(new Text("1111"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNull(oi.get("user_cvr_24hr"));
    Assert.assertNull(oi.get("ad_cvr_24hr"));
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT - 5 - 5 * 2 - 2 - 1, oi.size());
  }

  @Test
  public void featureValueWrongFormatUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION", "[[1111,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1111,3]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CONVERSION", "wrong_format");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_conversion_24hr", "wrong_format");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1111");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(new Text("1111"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNull(oi.get("user_cvr_24hr"));
    Assert.assertNull(oi.get("ad_cvr_24hr"));
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT - 5 - 5 * 2 - 2 - 1, oi.size());
  }

  @Test
  public void adInfosWrongFormatUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION", "[[1111,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1111,3]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CONVERSION", "wrong_format");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_conversion_24hr", "wrong_format");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1111");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(new Text("1111"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("wrong"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNull(oi.get("user_cvr_24hr"));
    Assert.assertNull(oi.get("ad_cvr_24hr"));
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_campaign_ctr_24hr"));
    Assert.assertEquals(1, oi.get("user_campaign_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT - 5 - 5 * 2 - 2 - 1, oi.size());
  }

  @Test
  public void adInfosPartMissingUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION", "[[1111,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[[1111,3]]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CONVERSION", "wrong_format");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_conversion_24hr", "wrong_format");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1111");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(new Text("1111"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNull(oi.get("user_cvr_24hr"));
    Assert.assertNull(oi.get("ad_cvr_24hr"));
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_campaign_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_campaign_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_account_ctr_24hr"));
    Assert.assertEquals(1, oi.get("user_account_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT - 5 - 5 * 2 - 2 - 1, oi.size());
  }

  @Test
  public void newVersionCompleteFeatureUDF() throws HiveException {
    GetUserAdRTCountersUDF udf = new GetUserAdRTCountersUDF();
    udf.initialize(inputOIs);

    Map<String, String> userFeatureMap = new HashMap<>();
    userFeatureMap.put(
        "USER_24HR_AD_EVENT_IMPRESSION",
        "[\"[[1577702671705346050,1],[2111, 10],[1112, 20],[1113,30],[1114,40]]\"]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CLICK", "[\"[[1577702671705346050,3]]\"]");
    userFeatureMap.put("USER_24HR_AD_EVENT_CONVERSION", "[\"[[1577702671705346050,5]]\"]");
    Map<String, String> adFeatureMap = new HashMap<>();
    adFeatureMap.put("ad_conversion_24hr", "[2.0]");
    adFeatureMap.put("ad_click_24hr", "[1026.0]");
    adFeatureMap.put("ad_show_24hr", "[49158.0]");

    Object userFeature = new Text(gson.toJson(userFeatureMap));
    Object adFeature = new Text(gson.toJson(adFeatureMap));
    Object adId = new Text("1577702671705346050");
    Object adInfos =
        Arrays.asList(
            Arrays.asList(
                new Text("1577702671705346050"), new Text("111"), new Text("11"), new Text("1")),
            Arrays.asList(new Text("2111"), new Text("111"), new Text("11"), new Text("1")));

    GenericUDF.DeferredObject[] argas = {
      new GenericUDF.DeferredJavaObject(userFeature),
      new GenericUDF.DeferredJavaObject(adFeature),
      new GenericUDF.DeferredJavaObject(adId),
      new GenericUDF.DeferredJavaObject(adInfos),
    };

    oi = (Map<String, Double>) udf.evaluate(argas);
    Assert.assertNotNull(oi.get("user_ctr_24hr"));
    Assert.assertEquals(0.2, oi.get("user_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("ad_ctr_24hr"));
    Assert.assertEquals(1026.0 / 49158., oi.get("ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_ad_ctr_24hr"));
    Assert.assertEquals(1.0, oi.get("user_ad_ctr_24hr"), DELTA);
    Assert.assertNotNull(oi.get("user_adset_ctr_24hr"));
    Assert.assertEquals(0.5, oi.get("user_adset_ctr_24hr"), DELTA);
    Assert.assertEquals(FULL_FEATURE_COUNT, oi.size());
  }
}
