package com.dtmobile.job;

import com.alibaba.fastjson.JSONObject;
import com.dtmobile.azkaban.AzkabanOperator;
import com.dtmobile.util.SSHHelper;

/**
 * Driver
 *
 * @author heyongjin
 * @create 2017/04/07 9:59
 **/

public class Driver {
    public static void main(String[] args) throws Exception {
        String exec = SSHHelper.exec(args[10], args[11], args[12], Integer.valueOf(args[13]),args[14]);
        System.out.println(exec);
        /*
        AzkabanOperator op = new AzkabanOperator(args[0], args[1], args[2], args[3], args[4], args[5], args[6], args[7]);
        JSONObject json = op.login();
        System.out.println(JSONObject.toJSONString(json));
        System.out.println(json.getString("session.id"));
        System.out.println(JSONObject.toJSONString(op.executeGdiFlow(json.getString("session.id"), args[8], args[9])));
        */
    }
}
