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

import javax.json.Json;
import javax.json.JsonObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
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
        Graph g = transaction.getGraph();

        // fetch iterator of movies connected to keyword
        GraphTraversal<Vertex, Vertex> movies = g.traversal().V().
                has("ITEM_IDENTIFIER", source).
                outE("RELATION").
                has("TO_TYPE", "http://mindmaps.io/movie").otherV();

        // insert relations of type keyword-concept
        movies.forEachRemaining(v -> {
            MindmapsGraph assertionTransaction = mg.buildTransaction(config);

            // get source and target concepts to check for errors in input
            if (!assertionTransaction.getConceptInstanceByItemIdentifier(source).getType()
                    .equals("http://mindmaps.io/keyword")) {throw new RuntimeException();}
//            if (assertionTransaction.getConceptInstanceByItemIdentifier(target)
//                    .equals(null)) {throw new RuntimeException();}

            try {
                // prepare post
                URL url = new URL("http://localhost:8080/transaction");
                HttpURLConnection con = (HttpURLConnection) url.openConnection();
                con.setDoInput(true);
                con.setDoOutput(true);
                con.setRequestProperty("Content-Type", "application/json");
                con.setRequestProperty("Accept", "application/json");
                con.setRequestMethod("POST");
                OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream());

                // create payload
                JsonObject post = createPost(v.value("ITEM_IDENTIFIER"), target);

                // send
                wr.write(post.toString());
                wr.flush();

                // display response

                StringBuilder sb = new StringBuilder();
                int HttpResult = con.getResponseCode();
                if (HttpResult == HttpURLConnection.HTTP_OK) {
                    BufferedReader br = new BufferedReader(
                            new InputStreamReader(con.getInputStream(), "utf-8"));
                    String line = null;
                    while ((line = br.readLine()) != null) {
                        sb.append(line + "\n");
                    }
                    br.close();
                    System.out.println("" + sb.toString());
                } else {
                    System.out.println(con.getResponseMessage());
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        // close transaction
        transaction.commit();
    }

    private static JsonObject createPost(String movie, String concept) {
        return Json.createObjectBuilder()
                .add("add",Json.createObjectBuilder()
                    .add("relationships",Json.createObjectBuilder()
                        .add("keyword-concept", Json.createArrayBuilder()
                            .add(Json.createObjectBuilder()
                                .add("roles", Json.createObjectBuilder()
                                    .add("related-concept",concept)
                                    .add("movie-with-keyword",movie)
                                )
                            )
                        )
                    )
                )
                .build();
    }
}
