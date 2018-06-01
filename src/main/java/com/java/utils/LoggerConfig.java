package com.java.utils;

import java.util.HashMap;
import java.util.Map;

public class LoggerConfig {

    public static Map<String,Map<String,String>> logs = new HashMap<String,Map<String,String>>(){{

        /**
         *  登录日志
         */
         put("login",new HashMap<String,String>(){{
               put("rddFilter","-1-{");
               put("strSplite","-1-\\{");
               put("collenctionName","login");
         }});

        /**
         *  玩家在线日志
         */
        put("online",new HashMap<String,String>(){{
            put("rddFilter","-2-{");
            put("strSplite","-2-\\{");
            put("collenctionName","online");
        }});
    }};




}
