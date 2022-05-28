package com.newsbreak.data.utils;

import com.nb.data.ab.bean.ABContext;

import java.util.List;
import java.util.Map;

public class ABFManager {

    static private ABFService abServices;

    static public void init(ABFManager.Env env, List<String> layerNames) throws Exception {
        synchronized (ABFManager.class) {
            if (abServices != null) {
                return;
            }
            abServices = new ABFService(env, layerNames);
        }
    }

    static public Map<String, String> abVersion(ABContext ctx) {
        return abServices.abVersion(ctx);
    }

    static public Map<String, String> abBucket(ABContext ctx) {
        return abServices.abBucket(ctx);
    }

    public enum Env {
        DEV("http://172.31.23.174:8220", "http://ab-admin-beta.stag.svc.k8sc1.nb.com:8081"),
        STAG("http://ab-api.stag.svc.k8sc1.nb.com:8220", "http://ab-admin.stag.newsbreak.com"),
        PROD("http://ab-api.ha.nb.com:8220", "http://ab-admin.n.newsbreak.com");

        private final String urlABApi;
        private final String urlABAdmin;

        Env(String urlABApi, String urlABAdmin) {
            this.urlABApi = urlABApi;
            this.urlABAdmin = urlABAdmin;
        }

        public String getUrlABApi() {
            return urlABApi;
        }
        public String getUrlABAdmin() {
            return urlABAdmin;
        }
    }
}
