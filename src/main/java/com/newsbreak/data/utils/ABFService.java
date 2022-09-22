package com.newsbreak.data.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nb.data.ab.bean.ABContext;
import com.nb.data.ab.bean.ABResult;
import com.nb.data.ab.bean.Layer;
import com.nb.data.ab.service.BucketService;
import com.nb.data.ab.service.IABService;
import com.nb.data.ab.service.UserService;
import org.apache.commons.codec.digest.MurmurHash3;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

public class ABFService {

    private static final Logger logger = LoggerFactory.getLogger(ABFService.class);

    private static final int BUCKET_SIZE = 1000;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ABFManager.Env env;

    private final List<Layer>       layers;
    private final List<IABService>  services;


    private String getMainClassName() {
        StackTraceElement[] trace = Thread.currentThread().getStackTrace();
        if (trace.length > 0) {
            return trace[trace.length - 1].getClassName();
        }
        return "default";
    }

    public ABFService(ABFManager.Env env, List<String> layerNames) throws Exception {
        this.env            = env;

        // step1
        List<Layer> layers0 = initLayersFromABAdmin(layerNames);
        this.layers         = layers0;

        // step2
        List<Layer> layers1 = initLayersFromABApi(layers0.stream().map(Layer::getName).collect(Collectors.toList()));
        this.services       = Arrays.asList(new UserService(layers1), new BucketService(layers1));
    }

    private List<Layer> initLayersFromABAdmin(List<String> layerNames) throws Exception {
        List<Layer> layers0 = null;
        URI uri = new URIBuilder()
                .setScheme("http")
                .setHost(env.getUrlABAdmin())
                .setPath("/api/v2/layers").build();

        HttpGet httpGet = new HttpGet(uri);
        httpGet.setConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(30000)
                .setSocketTimeout(30000)
                .setConnectTimeout(30000)
                .build()
        );
        httpGet.addHeader("Content-Type","application/json");
        httpGet.addHeader("x-user-email","nb.admin@newsbreak.com");
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = httpClient.execute(httpGet);

        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            throw new RuntimeException("status code: "+response.getStatusLine().getStatusCode());
        }

        HttpEntity httpEntity = response.getEntity();
        if(httpEntity == null){
            throw new RuntimeException("httpEntity is null");
        }

        String res = EntityUtils.toString(httpEntity,"UTF-8");
        EntityUtils.consume(httpEntity);
        // JSONObject jsonResult = JSONObject.fromObject(result);

        layers0 = objectMapper.readValue(res, new TypeReference<List<Layer>>() {});
        if (null != layerNames && layerNames.size() > 0) {
            layers0 = layers0.stream().filter(l -> layerNames.contains(l.getName())).collect(Collectors.toList());
        }

        layers0.forEach(Layer::init);
        logger.info("Load Layers From ABAdmin Success");

        return layers0;
    }

    private List<Layer> initLayersFromABApi(List<String> layerNames) {
        List<Layer> layers1 = null;
        URI uri = null;
        try {
            URIBuilder builder = new URIBuilder(env.getUrlABApi() + "/ab/newsbreak/layers/" + String.join(",", layerNames));
            builder.setParameter("app", getMainClassName())
                    .setParameter("client_version", Optional.ofNullable(getClass().getPackage().getImplementationVersion()).orElse("0.0.0"))
                    .setParameter("last_status", "success");

            uri = builder.build();

        } catch (URISyntaxException e) {
            logger.error("ab build uri error", e);
        }

        HttpGet get = new HttpGet(uri);
        get.setConfig(RequestConfig.custom()
                .setSocketTimeout(30000)
                .setConnectTimeout(30000)
                .build()
        );

        try (CloseableHttpClient httpClient = HttpClients.createDefault();
             CloseableHttpResponse response = httpClient.execute(get)){
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("status code: "+response.getStatusLine().getStatusCode());
            }

            layers1 = objectMapper.readValue(response.getEntity().getContent(), new TypeReference<List<Layer>>() {});
            layers1.forEach(Layer::init);

            logger.info("Load Layers From ABApi Success");
        } catch (Exception e) {
            logger.error("Load Layers From ABApi Failed", e);
        }
        return layers1;
    }

    public Map<String, String> abVersion(ABContext ctx) {
        ABResult result = new ABResult();
        services.forEach(service -> {
            service.ab(ctx, result);
        });
        return result.getExp();
    }

    public Map<String, String> abBucket(ABContext ctx) {
        Map<String, String> hitBuckets = new HashMap<>();
        layers.forEach(layer -> {
            StringBuilder sb = new StringBuilder(layer.getShufflePrefix());
            long ni = Integer.toUnsignedLong(MurmurHash3.hash32x86(sb.append("@").append(ctx.getFactor()).toString().getBytes()));
            hitBuckets.put(layer.getName(), String.format("%03d", ni % BUCKET_SIZE));
        });
        return hitBuckets;
    }
}
