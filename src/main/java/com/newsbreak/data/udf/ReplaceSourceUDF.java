package com.newsbreak.data.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ReplaceSourceUDF
 * @author yiduan
 */

@Description(name = "ReplaceSourceUDF", value = "ReplaceSourceUDF(features)",
        extended = "")
public class ReplaceSourceUDF extends GenericUDF {
    private ObjectInspectorConverters.Converter converter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("ReplaceSourceUDF(features)");
        }

        converter = ObjectInspectorConverters.getConverter(arguments[0],
                PrimitiveObjectInspectorFactory.writableStringObjectInspector);

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        assert (arguments.length == 1);

        if (arguments[0].get() == null) {
            return null;
        }

        Text features = (Text) converter.convert(arguments[0].get());

        final String regex = "h:hist_[^*]*\\*src[^*]*\\*(?<source>[^:]*):1\\.00000";
        final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(features.toString());

        StringBuffer sb = new StringBuffer(features.toString().length());
        while (matcher.find()) {
            String hist = matcher.group(0);
            String source = matcher.group("source");
            String newHist = hist.replace(source, normalize(source));
            matcher.appendReplacement(sb, newHist);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    @Override
    public String getDisplayString(String[] children) {
        assert (children.length == 1);
        return "ReplaceSourceUDF(*)";
    }

    private String normalize(String key) {
        if (key == null) {
            return null;
        }
        //"\\P{Graph}"表示只允许可见且非空的字符，包括字母数字和可见的英文标点字符. 外文，空格，控制字符都不允许。
        //
        // 正则表达式写法参考： https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html#sum
        // \p{Alpha}   An alphabetic character:[\p{Lower}\p{Upper}]
        // \p{Digit}   A decimal digit: [0-9]
        // \p{Alnum}   An alphanumeric character:[\p{Alpha}\p{Digit}]
        // \p{Punct}   Punctuation: One of !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
        // \p{Graph}   A visible character: [\p{Alnum}\p{Punct}]
        // \P{Graph} 是\p{Graph}的反集
        //标点字符的用途：#用来区分key的多个不同域， *用来区分一个域下的多个不同值， :用来区分key和value
        return key.toLowerCase().replaceAll("\\P{Graph}", "_").replace("|", "_").replace(":", "_");
    }
}
