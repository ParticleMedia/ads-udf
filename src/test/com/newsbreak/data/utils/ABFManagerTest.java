package com.newsbreak.data.utils;

import com.nb.data.ab.bean.ABContext;
import org.junit.Test;
import java.util.Map;

/**
 * Created on 2021/5/17.
 *
 * @author wei.liu
 */
public class ABFManagerTest {

    @Test
    public void init() {
    }

    // @Test
    public void abBucket() throws Exception {
        ABFManager.init(ABFManager.Env.PROD, null);
        Map<String, String> ret = ABFManager.abBucket(ABContext.create().withFactor("1111").build());
        System.out.print(ret.toString());
    }

    // @Test
    public void abVersion() throws Exception {
        ABFManager.init(ABFManager.Env.PROD, null);
        Map<String, String> ret = ABFManager.abVersion(ABContext.create().withFactor("1111").build());
        System.out.print(ret.toString());
    }

    @Test
    public void ab1000() {
    }
}