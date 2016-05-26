import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsGraphFactory;
import io.mindmaps.core.exceptions.ConceptException;
import io.mindmaps.core.structure.types.Assertion;
import io.mindmaps.core.structure.types.Casting;
import io.mindmaps.core.structure.types.ConceptInstance;
import io.mindmaps.graph.config.TitanGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.Map.Entry;

/**
 *
 */

public class main {

    public static void main(String [ ] args) throws InterruptedException {
        String graphConf = "/opt/mindmaps/resources/conf/titan-cassandra-es-batch.properties";

        // get iid maps in memory
        LoadExtractedData data = new LoadExtractedData();
        data.loadFromDisk();

        // read resolutions and map to iids
        PrepareResolutionMaps fix = new PrepareResolutionMaps(data);
        fix.prepareMaps();

        // get graph factory
        MindmapsGraphFactory mg = new TitanGraphFactory();

        // collect iterators for round robin load
        MindmapsGraph transaction = mg.buildTransaction(graphConf);
        Graph g = transaction.getGraph();
        Map<String,GraphTraversal<Vertex, Vertex>> iterators = new HashMap<>();

        fix.resolutionIIDMap.forEach((k, v) -> {
            ConceptInstance potentialKeyword = transaction.getConceptInstanceByItemIdentifier(k);
            if (potentialKeyword != null) {
                if (!potentialKeyword.getType()
                        .equals("http://mindmaps.io/keyword")) {
                    throw new RuntimeException();
                    }
                iterators.put(
                        k,
                        g.traversal().V().
                                has("ITEM_IDENTIFIER", k).
                                outE("RELATION").
                                has("TO_TYPE", "http://mindmaps.io/movie").otherV());
                }
            }
        );


        // perform fix of all keywords
        int i = 0;
        boolean hasNext = true;
        while (hasNext) {
            i++;
            hasNext = false;
            for (Entry<String, GraphTraversal<Vertex, Vertex>> entry : iterators.entrySet()) {
                if (entry.getValue().hasNext()) {
                    UpdateKeyword.execute(
                            fix.resolutionIIDMap.get(entry.getKey()),
                            entry.getValue().next().value("ITEM_IDENTIFIER"),
                            fix.resolutionRelationType.get(entry.getKey()));
                    hasNext |= entry.getValue().hasNext();
                }
                //TODO: remove the iterator once it is empty
            }
            System.out.println("Round robin completed: " + String.valueOf(i));
        }

        // delete the updated keywords
        for(String k: fix.resolutionIIDMap.keySet()){
            MindmapsGraph delete = mg.buildTransaction(graphConf);
            ConceptInstance keyword = delete.getConceptInstanceByItemIdentifier(k);
            if (keyword!=null) {
                System.out.println("deleting keyword: " + keyword.getValue().toString());

                try {
                    Set<Assertion> assertions = new HashSet<>();
                    Set<Casting> castings = keyword.getRoleCasting();
                    castings.forEach(c -> assertions.addAll(c.getAssertions()));

                    castings.forEach(c -> {
                        GraphTraversal<Vertex, Vertex> t = delete.getGraph().traversal().V(c.getBaseIdentifier());
                        if (t.hasNext())
                            t.next().remove();
                    });

                    assertions.forEach(a -> {
                        GraphTraversal<Vertex, Vertex> t = delete.getGraph().traversal().V(a.getBaseIdentifier());
                        if (t.hasNext())
                            t.next().remove();
                    });

                    keyword.delete();
                } catch (ConceptException e) {
                    e.printStackTrace();
                }
                List<String> errors = delete.commit();
                for (String error : errors)
                    System.out.println("Error: " + error);
                System.out.println("keyword deleted");
            }
        }

        System.out.println("keyword update done");
        System.exit(0);
        }
    }
