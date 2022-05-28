package com.newsbreak.data.udf;

import be.cylab.java.roc.Roc;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.Text;
/**
 * AUC UDF
 * @author xy
 */

@Description(name = "GetAuc", value = "GetAuc(concat) returns AUC computed for a series of true/false",
 extended = "")
public class GetAucUDF extends UDF {
 
  public Double evaluate(Text inputText){

    String inputStr = inputText.toString();
    String[] dataPoints = inputStr.split(", ");

    double[] scores = new double[dataPoints.length];
    boolean[] flags = new boolean[dataPoints.length];

    int i = 0;
    for (String data : dataPoints) {
      String[] tokens = data.split(":");
      double score = 1.0 / Integer.parseInt(tokens[0]);
      boolean flag = Boolean.parseBoolean(tokens[1]);
      scores[i] = score;
      flags[i] = flag;
      i++;
    }

    Roc roc = new Roc(scores, flags);
    return roc.computeAUC();
  }
}