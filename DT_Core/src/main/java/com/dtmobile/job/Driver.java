package com.dtmobile.job;

import com.alibaba.fastjson.JSONObject;
import com.dtmobile.azkaban.AzkabanOperator;
import com.dtmobile.util.SSHHelper;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * Driver
 *
 * @author zhangchao
 * @create 2017/04/07 9:59
 **/

public class Driver {
    /**
     * args1 className
     * args2  methodName
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
            Driver d =new Driver();
            String [] arrs = new String[]{"com.dtmobile.AzkabanOperator"};
            d.test(arrs);
    }

    public void test(String[] args) throws Exception{
        Class<?> clazz;
        clazz = Class.forName(args[0]);
        Method method;
        String exec;
        // 根据不同类调用不同的方法
        if(clazz.getName().equals(AzkabanOperator.class.getName())){
            method = clazz.getMethod(args[1],String.class,String.class,String.class,int.class,String.class);
            exec = method.invoke(clazz.newInstance(),args[2],args[3],args[4],Integer.parseInt(args[5]),args[6]).toString();
            System.out.println(exec);

        }else{
            method = clazz.getMethod(args[1],String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class);
            exec = method.invoke(clazz.newInstance(),args[2],args[3],args[4],args[5],args[6],args[7],args[8],args[9]).toString();
            JSONObject json = JSONObject.parseObject(exec);
            System.out.println(json.getString("session.id"));
            Map<String, String> paramMap = new HashMap<String, String>();
            for (int i = 11; i < args.length; i++) {
                paramMap.put(args[i], args[i++]);
                i++;
            }
            method = clazz.getMethod(args[10],Map.class);
            System.out.println(JSONObject.toJSONString(method.invoke(clazz.newInstance(),paramMap).toString()));
        }
    }
}

