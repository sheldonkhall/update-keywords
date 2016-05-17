import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import io.mindmaps.core.dao.GraphDAO;
import io.mindmaps.core.dao.GraphDAOImpl;
import io.mindmaps.graph.config.MindmapsGraphFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 */

public class main {
    public static void main(String [ ] args) throws InterruptedException {
        String graphConf = "/opt/mindmaps/resources/conf/titan-cassandra-es.properties";

        // get iid maps in memory
        LoadExtractedData data = new LoadExtractedData();
        data.loadFromDisk();

        // read resolutions and map to iids
        PrepareResolutionMaps fix = new PrepareResolutionMaps(data);
        fix.prepareIIDMaps();

        // start a thread pool to handle keywords in parallel
        ExecutorService threadPool = Executors.newFixedThreadPool(20);

        // perform fix of all keywords
        data.conceptsIID.get("keyword").forEach((k,v)->{
            v.forEach(iid -> {
                if (fix.resolutionIIDMap.containsKey(iid)) {
                    threadPool.execute(() -> UpdateKeyword.execute(iid, fix.resolutionIIDMap.get(iid), graphConf));
                } else {
                    threadPool.execute(()->UpdateKeyword.execute(iid, iid, graphConf));
                }
            });
        });

        threadPool.shutdown();
        threadPool.awaitTermination(3,TimeUnit.DAYS);
        System.exit(0);
    }
}
