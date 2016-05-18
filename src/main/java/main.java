import io.mindmaps.core.dao.MindmapsGraphFactory;
import io.mindmaps.graph.config.TitanGraphFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */

public class main {
    Set<String> postAssertions = ConcurrentHashMap.newKeySet();

    public static void main(String [ ] args) throws InterruptedException {
        String graphConf = "/opt/mindmaps/resources/conf/titan-cassandra-es-batch.properties";

        // get iid maps in memory
        LoadExtractedData data = new LoadExtractedData();
        data.loadFromDisk();

        // read resolutions and map to iids
        PrepareResolutionMaps fix = new PrepareResolutionMaps(data);
        fix.prepareIIDMaps();

        // get graph factory
        MindmapsGraphFactory mg = new TitanGraphFactory();

        // perform fix of all keywords
        data.conceptsIID.get("keyword").forEach((k,v)->
            v.forEach(iid -> {
                if (fix.resolutionIIDMap.containsKey(iid)) {
                    UpdateKeyword.execute(iid, fix.resolutionIIDMap.get(iid), mg, graphConf);
                } else {
                    UpdateKeyword.execute(iid, iid, mg, graphConf);
                }
            })
        );

        System.exit(0);
    }
}
