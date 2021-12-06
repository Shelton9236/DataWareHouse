package com.atguigu.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {

    public String evaluate(String line,String keysStr){
        String[] keysArr = keysStr.split(",");

        //原始时间日志格式：时间|json日志
        String[] logContent = line.split("\\|");
        if (logContent.length != 2 || StringUtils.isBlank(logContent[1])){
            return "";
        }

        StringBuffer sb = new StringBuffer();
        try {
            //拼接公共字段
            JSONObject jsonObject = new JSONObject(logContent[1]);
            JSONObject cm = jsonObject.getJSONObject("cm");
            for (int i = 0; i < keysArr.length; i++) {
                String key = keysArr[i].trim();
                if (cm.has(key)){
                    sb.append(cm.getString(key)).append("\t");
                }

            }

            //拼接事件字段
            sb.append(jsonObject.getString("et")).append("\t");

            //拼接服务器时间
            sb.append(logContent[0]).append("\t");


        } catch (JSONException e) {
            e.printStackTrace();
        }

        return sb.toString();
    }

}
