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
    public static void main(String[] args) {
      /*  AzkabanOperator op = new AzkabanOperator(args[0], args[1], args[2], args[3], args[4], args[5],args[6], args[7]);
        try {
            JSONObject json = op.login();
            System.out.println(JSONObject.toJSONString(json));
            System.out.println(json.getString("session.id"));
            System.out.println(JSONObject.toJSONString(op.executeGdiFlow(json.getString("session.id"), args[8], args[9])));
        } catch (Exception e) {
            e.printStackTrace();
        }*/
        String exec = SSHHelper.exec("namenode01", "hadoop", "hadoop", 22, "/home/hadoop/hdfs2local.sh;ls /home/hadoop");
        System.out.println(exec);
    }
}
