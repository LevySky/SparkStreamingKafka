package com.java.storage.logs;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

public class DocumentHelper {

    public static Document getByString(String str,String logSpliteType) {

        Document document = new Document();
        try {

            if (StringUtils.isEmpty(str)) {
                return document;
            }
            String[] splited = str.split(logSpliteType);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //sdf.setCalendar(new GregorianCalendar(new SimpleTimeZone(0, "GMT")));
            Map<String,Object> map = JSONObject.parseObject("{"+splited[1]);

            String rexp = "^.*time$";
            Pattern pattern = Pattern.compile(rexp,Pattern.CASE_INSENSITIVE);

            for(String  key : map.keySet()){

                Object value = map.get(key);
                if(pattern.matcher(key).find() && value.toString().length() > 11){
                    //System.out.println(key+value);
                    document.put(key, new Date((long)value));
                }else if("playerId".equals(key)){
                    document.put(key, value.toString());
                }else{
                    document.put(key, value);
                }

            }
            document.put("logDate", sdf.parse(splited[0].split(",")[0]));
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            return document;
        }
    }
}
