package com.dtmobile.azkaban;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import net.sf.json.JSONObject;

public class AzkabanHttpsPost {
    static String keystorePassword;
    static String keystore;
    static String truststore;

    static {
        String confDir = System.getProperty("user.dir")+"/conf/";
        System.out.println(confDir);
        InputStream is = Thread.currentThread().getContextClassLoader().
                getResourceAsStream(confDir+"azkaban.properties");
        Properties p = new Properties();
        try {
            p.load(is);
            keystorePassword = p.getProperty("PASSWORD");
            keystore = confDir+"keystore";
            truststore = keystore;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static KeyStore getKeyStore(String password, String storePath)
            throws Exception {
        // 实例化密钥库
        KeyStore ks = KeyStore.getInstance("JKS");
        // 获得密钥库文件流
        FileInputStream is = new FileInputStream(storePath);
        // 加载密钥库
        ks.load(is, password.toCharArray());
        // 关闭密钥库文件流
        is.close();
        return ks;
    }

    public static SSLContext getSSLContext() throws Exception {
        // 实例化密钥库
        KeyManagerFactory keyManagerFactory = KeyManagerFactory
                .getInstance(KeyManagerFactory.getDefaultAlgorithm());
        // 获得密钥库
        KeyStore keyStore = getKeyStore(AzkabanHttpsPost.keystorePassword, AzkabanHttpsPost.keystore);
        // 初始化密钥工厂
        keyManagerFactory.init(keyStore, AzkabanHttpsPost.keystorePassword.toCharArray());
        // 实例化信任库
        TrustManagerFactory trustManagerFactory = TrustManagerFactory
                .getInstance(TrustManagerFactory.getDefaultAlgorithm());
        // 获得信任库
        KeyStore trustStore = getKeyStore(AzkabanHttpsPost.keystorePassword, AzkabanHttpsPost.truststore);
        // 初始化信任库
        trustManagerFactory.init(trustStore);
        // 实例化SSL上下文
        SSLContext ctx = SSLContext.getInstance("TLS");
        // 初始化SSL上下文
        ctx.init(keyManagerFactory.getKeyManagers(),
                trustManagerFactory.getTrustManagers(), null);
        // 获得SSLSocketFactory
        return ctx;
    }


    public static void initHttpsURLConnection() throws Exception {
        // 声明SSL上下文
        SSLContext sslContext = null;
        // 实例化主机名验证接口
        HostnameVerifier hnv = new MyHostnameVerifier();
        try {
            sslContext = getSSLContext();
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }
        if (sslContext != null) {
            HttpsURLConnection.setDefaultSSLSocketFactory(sslContext
                    .getSocketFactory());
        }
        HttpsURLConnection.setDefaultHostnameVerifier(hnv);
    }

    public static JSONObject post(String url, String xmlStr) throws Exception {
        initHttpsURLConnection();
        JSONObject jsonObj = null;
        HttpsURLConnection urlCon = null;
        try {
            urlCon = (HttpsURLConnection) (new URL(url)).openConnection();
            urlCon.setDoInput(true);
            urlCon.setDoOutput(true);
            urlCon.setRequestMethod("POST");
            // 如下设置后，azkaban才能识别出是以ajax的方式访问，从而返回json格式的操作信息
            urlCon.setRequestProperty("Content-Type",
                    "application/x-www-form-urlencoded");
            urlCon.setRequestProperty("X-Requested-With", "XMLHttpRequest");
            urlCon.setUseCaches(true);
            // 设置为gbk可以解决服务器接收时读取的数据中文乱码问题
            urlCon.getOutputStream().write(xmlStr.getBytes("gbk"));
            urlCon.getOutputStream().flush();
            urlCon.getOutputStream().close();
            BufferedReader in = new BufferedReader(new InputStreamReader(
                    urlCon.getInputStream()));
            String line = "";
            String temp;
            while ((temp = in.readLine()) != null) {
                line = line + temp;
            }
            jsonObj = JSONObject.fromObject(line);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonObj;
    }
}
