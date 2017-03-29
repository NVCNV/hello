package com.dtmobile.azkaban;

import java.io.InputStream;
import java.util.Properties;

import net.sf.json.JSONObject;
/**
 *
 * @author
 * @create 2017-03-28 14:38
 **/
public class AzkabanOperator {
    public static String url;
    public static String azkabanUser;
    public static String azkabanPassword;
    public static String GDI_Project;
    public static String GDI_Workflow;
    static {
        String confDir = System.getProperty("user.dir")+"/conf/";
        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(confDir+"azkaban.properties");
        Properties p = new Properties();
        try {
            p.load(is);
            url = p.getProperty("URL");
            azkabanUser = p.getProperty("AZKABANUSER");
            azkabanPassword = p.getProperty("AZKABANPASSWORD");
            GDI_Project = p.getProperty("GDI_Project");
            GDI_Workflow = p.getProperty("GDI_Workflow");
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

    public JSONObject fetchFlow(String sessionID, String execID)
            throws Exception {
        JSONObject result = null;
        String executeStr = "session.id=" + sessionID
                + "&ajax=fetchexecflow&execid=" + execID;
        String executeUrl = url + "/executor";
        result = AzkabanHttpsPost.post(executeUrl, executeStr);
        return result;
    }

    public static void main(String[] args) {
        AzkabanOperator op = new AzkabanOperator();
        try {
            System.out.println(JSONObject.fromObject(op.login()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
