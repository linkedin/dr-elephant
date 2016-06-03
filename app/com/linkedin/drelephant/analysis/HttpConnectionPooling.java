package com.linkedin.drelephant.analysis;

import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HttpConnectionPooling {
    private static HttpConnectionPooling ourInstance = new HttpConnectionPooling();

    public PoolingHttpClientConnectionManager getCm() {
        return cm;
    }

    private PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();

    public static HttpConnectionPooling getInstance() {
        return ourInstance;
    }

    private HttpConnectionPooling() {
        cm.setMaxTotal(96);
        cm.setDefaultMaxPerRoute(96);
    }
}
