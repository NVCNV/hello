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
 * @create 2017/04/07 9:59
 **/

public class Driver {
    public static void main(String[] args) throws Exception {
        if(args.length==5){
            String exec = SSHHelper.exec(args[10], args[11], args[12], Integer.valueOf(args[13]),args[14]);
            System.out.println(exec);
        }else{
            AzkabanOperator op = new AzkabanOperator(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args [7]);
            JSONObject json = op.login();
            System.out.println(JSONObject.toJSONString(json));
            System.out.println(json.getString("session.id"));
            Map<String, String> paramMap = new HashMap<String, String>();
            for (int i = 9; i < args.length; i++){
                paramMap.put(args[i], args[i++]);
                i++;
            }
            System.out.println(JSONObject.toJSONString(op.executeGdiFlow(json.getString("session.id"),paramMap)));
        }
    }
}
