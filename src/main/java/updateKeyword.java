import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import io.mindmaps.core.dao.GraphDAO;
import io.mindmaps.core.dao.GraphDAOImpl;
import io.mindmaps.core.structure.types.ConceptInstance;
import io.mindmaps.core.structure.types.RelationType;
import io.mindmaps.core.structure.types.RoleType;
import io.mindmaps.graph.config.MindmapsGraphFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */

public class UpdateKeyword {
    /**
     * This function takes movies attached to the source and creates the mm:related-concept relationship between those
     * movies and the concept indicated by the target.
     *
     * @param source an iid for a keyword
     * @param target and iid for a concept instance
     * @param tf a titan factory for getting transactions
     */
    public static void execute(String source, String target, String config){
        // get transaction
        GraphDAO mg = new GraphDAOImpl(MindmapsGraphFactory.buildNewTransaction(config));

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

        // commit changes
        mg.commit();
        System.out.println("finished: "+source);
    }
}
