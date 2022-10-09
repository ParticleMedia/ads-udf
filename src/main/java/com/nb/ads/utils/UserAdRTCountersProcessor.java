package com.nb.ads.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserAdRTCountersProcessor {
  private static final Gson gson = new Gson();
  private static final String[] LEVEL_NAMES = {"ad", "adset", "campaign", "account"};
  private static Map<String, String> USER_FEATURE_EVENT_MAP;
  private static final Map<String, String[]> FEATURE_RATIO_NAME_MAP;

  static {
    USER_FEATURE_EVENT_MAP = new HashMap<>();
    USER_FEATURE_EVENT_MAP.put("IMPRESSION", "show");
    USER_FEATURE_EVENT_MAP.put("CLICK", "click");
    USER_FEATURE_EVENT_MAP.put("CONVERSION", "conversion");
    FEATURE_RATIO_NAME_MAP = new HashMap<>();
    FEATURE_RATIO_NAME_MAP.put("ctr", new String[] {"click", "show"});
    FEATURE_RATIO_NAME_MAP.put("cvr", new String[] {"conversion", "click"});
    FEATURE_RATIO_NAME_MAP.put("ctcvr", new String[] {"conversion", "show"});
  }

  public static Map<String, Double> computeCounters(
      String userFeature, String adFeature, Map<Long, List<Long>> adInfos, Long adId) {
    Map<String, Double> result = new HashMap<>();
    result.putAll(UserAdRTCountersProcessor.getAdFeatureMap(adFeature));
    Map<String, Long[][]> userEventList = deserializedUserFeature(userFeature);
    Map<String, List<List<Long>>> augmentedUserEventList = augmentEventList(userEventList, adInfos);
    List<Long> adInfo = adInfos.get(adId);
    result.putAll(aggregateUserFeature(augmentedUserEventList, adInfo));
    result.putAll(computeRatio(result, "ad"));
    result.putAll(computeRatio(result, "user"));
    for (String levelName : LEVEL_NAMES) {
      result.putAll(computeRatio(result, String.format("user_%s", levelName)));
    }

    return result;
  }

  public static Map<String, Long[][]> deserializedUserFeature(String userFeature) {
    Map<String, Long[][]> userEventList = new HashMap<>();
    JsonParser parser = new JsonParser();
    JsonObject obj = parser.parse(userFeature).getAsJsonObject();
    for (Map.Entry<String, String> userEventName : USER_FEATURE_EVENT_MAP.entrySet()) {
      String key = String.format("USER_24HR_AD_EVENT_%s", userEventName.getKey());
      JsonElement ele = obj.get(key);
      if (ele == null) {
        continue;
      }
      try {
        userEventList.put(
            userEventName.getValue(), gson.fromJson(ele.getAsString(), Long[][].class));
      } catch (JsonParseException e) {
        // continue for the next event, no logging for now
      }
    }
    return userEventList;
  }

  public static Map<String, List<List<Long>>> augmentEventList(
      Map<String, Long[][]> eventLists, Map<Long, List<Long>> adInfos) {
    Map<String, List<List<Long>>> augmentedEventList = new HashMap<>();
    for (Map.Entry<String, Long[][]> event : eventLists.entrySet()) {
      String eventName = event.getKey();
      Long[][] events = event.getValue();
      List<List<Long>> augmentedEvents = new ArrayList<>();
      for (Long[] adTsPair : events) {
        Long adId = adTsPair[0];
        List<Long> adInfo = adInfos.get(adId);
        if (adInfo == null) {
          // add it to compute user level features.
          augmentedEvents.add(Arrays.asList(adId));
          continue;
        }
        augmentedEvents.add(adInfo);
      }
      augmentedEventList.put(eventName, augmentedEvents);
    }
    return augmentedEventList;
  }

  public static Map<String, Double> aggregateUserFeature(
      Map<String, List<List<Long>>> eventLists, List<Long> adInfo) {
    Map<String, Double> userFeatureMap = new HashMap<>();

    for (Map.Entry<String, List<List<Long>>> event : eventLists.entrySet()) {
      String eventName = event.getKey();
      List<List<Long>> events = event.getValue();
      for (List<Long> eventInfo : events) {
        String featureName = String.format("user_%s_24hr", eventName);
        userFeatureMap.put(featureName, userFeatureMap.getOrDefault(featureName, 0.) + 1.);
        if (eventInfo.size() == 1) {
          // eventInfo.ad.account != adInfo.account
          continue;
        }
        for (int i = 0; i < LEVEL_NAMES.length; ++i) {
          if (i >= eventInfo.size() || i >= adInfo.size()) {
            // some part is missing
            break;
          }
          if (eventInfo.get(i) != adInfo.get(i)) {
            // try to match the upper level
            continue;
          }
          String levelName = LEVEL_NAMES[i];
          featureName = String.format("user_%s_%s_24hr", levelName, eventName);
          userFeatureMap.put(featureName, userFeatureMap.getOrDefault(featureName, 0.) + 1.);
        }
      }
    }
    return userFeatureMap;
  }

  public static Map<String, Double> computeRatio(Map<String, Double> features, String prefix) {
    Map<String, Double> cxrs = new HashMap<>();
    for (Map.Entry<String, String[]> kv : FEATURE_RATIO_NAME_MAP.entrySet()) {
      String denominatorKey = String.format("%s_%s_24hr", prefix, kv.getValue()[1]);
      String numeratorKey = String.format("%s_%s_24hr", prefix, kv.getValue()[0]);
      Double denominator = features.get(denominatorKey);
      Double numerator = features.get(numeratorKey);
      if (denominator != null && numerator != null && denominator != 0.) {
        String cxrName = String.format("%s_%s_24hr", prefix, kv.getKey());
        cxrs.put(cxrName, numerator / denominator);
      }
    }
    return cxrs;
  }

  public static Map<String, Double> getAdFeatureMap(String adFeature) {
    Map<String, Double> adFeatureMap = new HashMap<>();
    JsonParser parser = new JsonParser();
    JsonObject obj = parser.parse(adFeature).getAsJsonObject();
    for (String featureEvent : USER_FEATURE_EVENT_MAP.values()) {
      String key = String.format("ad_%s_24hr", featureEvent);
      JsonElement ele = obj.get(key);
      if (ele == null) {
        continue;
      }
      try {
        Double[] values = gson.fromJson(ele.getAsString(), Double[].class);
        if (values.length > 0) {
          adFeatureMap.put(key, values[0]);
        }
      } catch (JsonParseException e) {
        // continue for the next event, no logging for now
      }
    }
    return adFeatureMap;
  }
}
