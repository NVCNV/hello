package com.dtmobile.job;

import com.alibaba.fastjson.JSONObject;
import com.dtmobile.azkaban.AzkabanOperator;
import com.dtmobile.util.SSHHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * Driver
 *
 * @author heyongjin
 * create 2017/04/07 9:59
 **/

public class Driver {
    public static void main(String[] args) throws Exception {
        if(args.length<=6){
            String exec = SSHHelper.exec(args[0], args[1], args[2], Integer.valueOf(args[3]),args[4]);
            System.out.println(exec);
        }else{
            AzkabanOperator op = new AzkabanOperator(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args [7]);
            JSONObject json = op.login();
            System.out.println(JSONObject.toJSONString(json));
            System.out.println(json.getString("session.id"));
            Map<String, String> paramMap = new HashMap<String, String>();
            System.out.println("args.length"+args.length);
            for (int i = 8; i < args.length; i++){
                paramMap.put(args[i], args[++i]);
//                System.out.println(args[i]+"\t"+args[++i]);
            }
            System.out.println(JSONObject.toJSONString(op.executeGdiFlow(json.getString("session.id"),paramMap)));
        }
    }
}
