package com.newsbreak.data.udf;

import java.util.ArrayList;

//import javolution.text.Text;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;
/**
 * JSONArray UDF
 * @author will
 */

@Description(name = "JsonArray", value = "JsonArray(json, path) returns an array cotaining objects in JSON, specified by PATH",
 extended = "JsonArray(json, path) returns an array cotaining objects in JSON, specified by PATH")
public class JsonArrayUDF extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function JsonArray(s, path_to_array) takes exactly 2 arguments.");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory
            .writableStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 2);

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    Text s = (Text) converters[0].convert(arguments[0].get());
    Text resource = (Text) converters[1].convert(arguments[1].get());

    ArrayList<Text> result = new ArrayList<Text>();
    JSONObject obj = null;
    try {
      obj = new JSONObject(s.toString());
    } catch (JSONException e) {
      e.printStackTrace();
    }
    String[] names = resource.toString().split("\\.");

    try {
      traverse(result, obj, names, 0);
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return result;
  }

  /** Performs a DFS traversal on the JSON object/array specified
  *   by X, and adding objects specified by NAMES to arraylist RES
  */
  private static void traverse(ArrayList<Text> res, Object x, String[] names, int index) throws JSONException {
      if (index  == names.length) {
        if (x instanceof JSONArray) {
          JSONArray arr = (JSONArray) x;
          for (int i = 0; i < arr.length(); i += 1) {
              res.add(new Text(arr.get(i).toString()));
            }
        } else {
          res.add(new Text((((JSONObject) x).toString())));
        }
        return;

      } else if (x instanceof JSONArray) {
          JSONArray arr = (JSONArray) x;
          boolean flag = false;
          try {
            arr.getJSONObject(0).getJSONObject(names[index]);
          } catch (Exception e) {
            flag = true;
          }
          for (int i = 0; i < arr.length(); i += 1) {
            if (flag) {
                traverse(res, arr.getJSONObject(i).getJSONArray(names[index]), names, index + 1);
              } else {
                traverse(res, arr.getJSONObject(i).getJSONObject(names[index]), names, index + 1);
              }
          } 
      } else {
        JSONObject temp = (JSONObject) x;
        boolean flag = false;
        try {
          temp.getJSONObject(names[index]);
        } catch (Exception e) {
          flag = true;
          try {
            temp.getJSONArray(names[index]);
          } catch (Exception exception) {
            //Path does not exist
            return;
          }
        }
        if (!flag) {
          traverse(res, temp.getJSONObject(names[index]), names, index + 1);
        } else {
          traverse(res, temp.getJSONArray(names[index]), names, index + 1);
        }
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "JsonArrayUDF(" + children[0] + ", " + children[1] + ")";
  }

}