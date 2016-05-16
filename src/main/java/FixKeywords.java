import com.thinkaurelius.titan.core.TitanFactory;
import io.mindmaps.core.dao.GraphDAO;
import io.mindmaps.core.dao.GraphDAOImpl;
import io.mindmaps.core.structure.types.ConceptInstance;
import io.mindmaps.core.structure.types.RelationType;
import io.mindmaps.core.structure.types.RoleType;
import io.mindmaps.graph.config.MindmapsGraphFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */

public class FixKeywords {
    LoadExtractedData data;
    Map<String,String> resolutionIIDMap;
    static String graphConf = "/opt/mindmaps/resources/conf/titan-cassandra-es.properties";
    GraphDAO mg;

    public FixKeywords (LoadExtractedData d) {
        data = d;
        mg = new GraphDAOImpl(TitanFactory.open(graphConf));
    }

    public void prepareIIDMaps(){
        // create the map for keywords to be updated
        resolutionIIDMap = new HashMap<>();
        data.resolutions.forEach((k,v)->{
            Set<String> iids = data.conceptsIID.get("keyword").get(k);
            if (iids!=null) {
                if (iids.size() == 1) {
                    String keywordIID = iids.iterator().next();
                    for (String value : v) {
                        Set<String> replacementIIDS = data.conceptsIID.get(value).get(k);
                        if (replacementIIDS!=null) {
                            if (replacementIIDS.size() == 1) {
                                resolutionIIDMap.put(keywordIID, replacementIIDS.iterator().next());
                            }
                        }
                    }
                }
            }
        });
    }

    /**
     * This function takes movies attached to the source and creates the mm:related-concept relationship between those
     * movies and the concept indicated by the target.
     *
     * @param source an iid for a keyword
     * @param target and iid for a concept instance
     */
    public void updateKeyword(String source, String target){
        // get source and target concepts
        ConceptInstance keyword = mg.getConceptInstanceByItemIdentifier(source);
        if (!keyword.getType().equals("http://mindmaps.io/keyword")) {throw new RuntimeException();}
        ConceptInstance targetConcept = mg.getConceptInstanceByItemIdentifier(target);

        // get ontology elements
        RelationType keywordConcept = mg.getRelationTypeByItemIdentifier("http://mindmaps.io/keyword-concept");
        RoleType relatedConcept = mg.getRoleTypeByItemIdentifier("http://mindmaps.io/related-concept");
        RoleType movieWithKeyword = mg.getRoleTypeByItemIdentifier("http://mindmaps.io/movie-with-keyword");

        Set<ConceptInstance> movies = keyword.getRelatedConceptInstancesOutgoing();
        movies.forEach(k -> {
            if (k.getType().equals("http://mindmaps.io/movie")) {
                Map<RoleType,ConceptInstance> roleMap = new HashMap();
                roleMap.put(movieWithKeyword,k);
                roleMap.put(relatedConcept,targetConcept);
                mg.putAssertion(keywordConcept,roleMap);
                System.out.println(k.getType());
            }
        });
    }
}
