import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsGraphFactory;
import io.mindmaps.core.structure.types.ConceptInstance;
import io.mindmaps.graph.config.TitanGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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

        // get graph factory
        MindmapsGraphFactory mg = new TitanGraphFactory();

        // collect iterators for round robin load
        MindmapsGraph transaction = mg.buildTransaction(graphConf);
        Graph g = transaction.getGraph();
        Map<String,GraphTraversal<Vertex, Vertex>> iterators = new HashMap<>();
//        System.out.println(data.conceptsIID.get("keyword").size());
//        System.out.println(data.conceptsIID.get("keyword"));
        data.conceptsIID.get("keyword").forEach((k,v)->
            v.forEach(iid -> {
                // confirm that we have a keyword
//                System.out.println(iid);
                ConceptInstance potentialKeyword = transaction.getConceptInstanceByItemIdentifier(iid);
                if (potentialKeyword != null) {
                    if (!potentialKeyword.getType()
                            .equals("http://mindmaps.io/keyword")) {
                        throw new RuntimeException();
                    }

                    // collect iterators
                    if (fix.resolutionIIDMap.containsKey(iid)) {
                        iterators.put(iid, g.traversal().V().
                                has("ITEM_IDENTIFIER", fix.resolutionIIDMap.get(iid)).
                                outE("RELATION").
                                has("TO_TYPE", "http://mindmaps.io/movie").otherV());
                    } else {
                        iterators.put(iid, g.traversal().V().
                                has("ITEM_IDENTIFIER", iid).
                                outE("RELATION").
                                has("TO_TYPE", "http://mindmaps.io/movie").otherV());
                    }
                }
            })
        );

        // perform fix of all keywords
        int i = 0;
        boolean hasNext = true;
        while (hasNext) {
            i++;
            hasNext = false;
            for (Entry<String,GraphTraversal<Vertex, Vertex>> entry : iterators.entrySet()) {
                if (entry.getValue().hasNext()) {
                    UpdateKeyword.execute(entry.getKey(),
                            entry.getValue().next().value("ITEM_IDENTIFIER"));
                    hasNext |= entry.getValue().hasNext();
                }
                //TODO: remove the iterator once it is empty
            }
            System.out.println("Round robin completed: "+String.valueOf(i));
        }

        System.exit(0);
    }
}
