package com.dtmobile.azkaban;


import com.alibaba.fastjson.JSONObject;

import java.util.Map;

public class AzkabanOperator {
    static String url;
    static String azkabanUser;
    static String azkabanPassword;
    static String GDI_Project;
    static String GDI_Workflow;
    static String keystorePassword;
    static String keystore;
    static String truststore;

    public AzkabanOperator(String url, String azkabanUser, String azkabanPassword, String gdi_Project, String gdi_Workflow
            , String keystorePassword, String keystore, String truststore) {
        try {
            this.url = url;
            this.azkabanUser = azkabanUser;
            this.azkabanPassword = azkabanPassword;
            this.GDI_Project = gdi_Project;
            this.GDI_Workflow = gdi_Workflow;
            this.keystorePassword = keystorePassword;
            this.keystore = keystore;
            this.truststore = truststore;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public JSONObject login() throws Exception {
        JSONObject result = null;
        String queryStr = "action=login&username=" + azkabanUser + "&password="
                + azkabanPassword;
        result = AzkabanHttpsPost.post(url, queryStr);
        return result;
    }


    public JSONObject executeGDIFlow(String sessionID, String project,
                                     String flow, String cwParams, String smParams, String gdiParams)
            throws Exception {
        JSONObject result = null;
        String executeStr = "session.id=" + sessionID
                + "&ajax=executeFlow&project=" + project + "&flow=" + flow
                + "&flowOverride[cw_params]=" + cwParams
                + "&flowOverride[sm_params]=" + smParams
                + "&flowOverride[gdi_params]=" + gdiParams;
        String executeUrl = url + "/executor";
        result = AzkabanHttpsPost.post(executeUrl, executeStr);
        return result;
    }

    public JSONObject executeGdiFlow(String sessionID, String dt, String hour)
            throws Exception {
        JSONObject result = null;
        String executeStr = "session.id=" + sessionID
                + "&ajax=executeFlow&project=" + GDI_Project + "&flow=" + GDI_Workflow
                + "&flowOverride[ANALY_DATE]=" + dt
                + "&flowOverride[ANALY_HOUR]=" + hour;
        String executeUrl = url + "/executor";
        result = AzkabanHttpsPost.post(executeUrl, executeStr);
        return result;
    }

    public JSONObject executeGdiFlow(String sessionID, Map<String,String> paramMap)
            throws Exception {
        JSONObject result = null;
        StringBuffer executeStr = new StringBuffer();
        executeStr.append("session.id=").append(sessionID)
                .append("&ajax=").append("executeFlow").append("&project=").append(GDI_Project).append("&flow=").append(GDI_Workflow);
        for(Map.Entry<String, String> entry:paramMap.entrySet()){
            executeStr.append("&").append("flowOverride[").append(entry.getKey()).append("]").append("=").append(entry.getValue());
        }
        String executeUrl = url + "/executor";
        result = AzkabanHttpsPost.post(executeUrl, executeStr.toString());
        return result;
    }

    public JSONObject fetchFlow(String sessionID, String execID)
            throws Exception {
        JSONObject result = null;
        String executeStr = "session.id=" + sessionID
                + "&ajax=fetchexecflow&execid=" + execID;
        String executeUrl = url + "/executor";
        result = AzkabanHttpsPost.post(executeUrl, executeStr);
        return result;
    }

    /**
     *"https://172.30.4.222:8443",
     * "azkaban",
     * "azkaban",
     * "Test",
     * "JobComplete",
     * "azkaban",
     * "E:\\keystore",
     * "E:\\keystore"
     * ANALY_DATE
     * ANALY_HOUR
     */
    public static void main(String[] args) {
        AzkabanOperator op = new AzkabanOperator(args[0], args[1], args[2], args[3], args[4], args[5],args[6], args[7]);
        try {
            JSONObject json = op.login();
            System.out.println(JSONObject.toJSONString(json));
            System.out.println(json.getString("session.id"));
            System.out.println(JSONObject.toJSONString(op.executeGdiFlow(json.getString("session.id"), args[8], args[9])));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

