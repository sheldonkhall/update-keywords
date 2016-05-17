import io.mindmaps.core.dao.MindmapsGraph;
import io.mindmaps.core.dao.MindmapsGraphFactory;
import io.mindmaps.core.dao.MindmapsGraphImpl;
import io.mindmaps.core.structure.types.ConceptInstance;
import io.mindmaps.core.structure.types.RelationType;
import io.mindmaps.core.structure.types.RoleType;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

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
     * @param config a titan config file
     */
    public static void execute(String source, String target, MindmapsGraphFactory mg, String config){
        // get graph transaction
        MindmapsGraph transaction = mg.buildTransaction(config);
//        transaction.enableBatchLoading();

        Graph g = transaction.getGraph();

        // fetch iterator of movies connected to keyword
        GraphTraversal<Vertex, Vertex> movies = g.traversal().V().
                has("ITEM_IDENTIFIER", source).
                outE("RELATION").
                has("TO_TYPE", "http://mindmaps.io/movie").otherV();

        // insert relations of type keyword-concept
        Map<RoleType, ConceptInstance> roleMap = new HashMap<>();
        movies.forEachRemaining(v -> {
            MindmapsGraph assertionTransaction = mg.buildTransaction(config);
            assertionTransaction.enableBatchLoading();

            // get source and target concepts
            ConceptInstance keyword = assertionTransaction.getConceptInstanceByItemIdentifier(source);
            if (!keyword.getType().equals("http://mindmaps.io/keyword")) {throw new RuntimeException();}
            ConceptInstance targetConcept = assertionTransaction.getConceptInstanceByItemIdentifier(target);

            // get ontology elements
            RelationType keywordConcept = assertionTransaction.getRelationTypeByItemIdentifier("http://mindmaps.io/keyword-concept");
            RoleType relatedConcept = assertionTransaction.getRoleTypeByItemIdentifier("http://mindmaps.io/related-concept");
            RoleType movieWithKeyword = assertionTransaction.getRoleTypeByItemIdentifier("http://mindmaps.io/movie-with-keyword");

            ConceptInstance k = assertionTransaction.getConceptInstanceByItemIdentifier(v.value("ITEM_IDENTIFIER"));
            roleMap.clear();
            roleMap.put(movieWithKeyword, k);
            roleMap.put(relatedConcept, targetConcept);
            assertionTransaction.putAssertion(keywordConcept, roleMap);
            assertionTransaction.commit();
        });

        // commit changes
        transaction.commit();
        System.out.println("finished: " + source);
    }
}
