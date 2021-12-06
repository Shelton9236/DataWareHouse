package com.atguigu.udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class EventJsonUDTF extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldTypes = new ArrayList<>();

        fieldNames.add("event_name");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        fieldNames.add("event_json");
        fieldTypes.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldTypes);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        //获取输入数据
        String input = objects[0].toString();

        if (StringUtils.isBlank(input)){
            return;
        }else {
            try {
                JSONArray ja = new JSONArray(input);
                String[] result = new String[2];

                for (int i = 0; i < ja.length(); i++) {

                    try {
                        result[0] = ja.getJSONObject(i).getString("en");
                        result[1] = ja.getString(i);
                    } catch (JSONException e) {
                        //防止因为某个数据的错误结束整个循环
                        continue;
                    }
                }

                //进来一行数据，返回2列多行数据
                forward(result);

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void close() throws HiveException {

    }
}
