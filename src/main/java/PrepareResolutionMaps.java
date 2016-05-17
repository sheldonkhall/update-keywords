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

public class PrepareResolutionMaps {
    LoadExtractedData data;
    Map<String,String> resolutionIIDMap;

    public PrepareResolutionMaps(LoadExtractedData d) {
        data = d;
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
}
