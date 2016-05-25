import javax.json.Json;
import javax.json.JsonObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.net.URL;

/**
 *
 */

public class UpdateKeyword {
    /**
     * This function takes movies attached to the source and creates the mm:related-concept relationship between those
     * movies and the concept indicated by the target.
     *
     * @param concept an iid for a concept instance to be attached to a movie
     * @param movie and iid for a concept instance that is a movie
     */
    public static void execute(String concept, String movie, String relationType){

        // insert relations of type keyword-concept
        try {
            // create payload
            JsonObject post = createPost(movie, concept, relationType);

            // prepare post
            // TODO deploy needs port 80
            URL url = new URL("http://localhost:80/transaction");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setDoInput(true);
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Accept", "application/json");
            con.setRequestMethod("POST");
            try {
                OutputStreamWriter wr = new OutputStreamWriter(con.getOutputStream());

                // send
                wr.write(post.toString());
                wr.flush();
            } catch (NoRouteToHostException e) {
                e.printStackTrace();
                System.out.println("Traffic too high - Failed to commit post: "+post);
            }

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

            // close connection
            con.disconnect();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static JsonObject createPost(String movie, String concept, String relationType) {
        JsonObject post = null;
        switch (relationType) {
            case "person": post = createPostPerson(movie, concept);
                break;
            case "location": post = createPostLocation(movie, concept);
                break;
            case "genre": post = createPostGenre(movie, concept);
                break;
            case "mood": post = createPostMood(movie, concept);
                break;
        }
        if (post==null) {
            System.out.println("post was null for: movie - "+movie+" and concept - "+concept);
        }
        return post;
    }

    private static JsonObject createPostPerson(String movie, String concept) {
        return Json.createObjectBuilder()
            .add("add", Json.createObjectBuilder()
                .add("relationships", Json.createObjectBuilder()
                    .add("person-via-keyword", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("roles", Json.createObjectBuilder()
                                .add("keyword-person", concept)
                                .add("movie-with-keyword-person", movie)
                            )
                        )
                    )
                )
            )
            .build();
    }

    private static JsonObject createPostLocation(String movie, String concept) {
        return Json.createObjectBuilder()
            .add("add", Json.createObjectBuilder()
                .add("relationships", Json.createObjectBuilder()
                    .add("has-location", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("roles", Json.createObjectBuilder()
                                .add("location-of-subject", concept)
                                .add("subject-with-location", movie)
                            )
                        )
                    )
                )
            )
            .build();
    }

    private static JsonObject createPostGenre(String movie, String concept) {
        return Json.createObjectBuilder()
            .add("add", Json.createObjectBuilder()
                .add("relationships", Json.createObjectBuilder()
                    .add("has-genre", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("roles", Json.createObjectBuilder()
                                .add("genre-of-production", concept)
                                .add("production-with-genre", movie)
                            )
                        )
                    )
                )
            )
            .build();
    }

    private static JsonObject createPostMood(String movie, String concept) {
        return Json.createObjectBuilder()
            .add("add", Json.createObjectBuilder()
                .add("relationships", Json.createObjectBuilder()
                    .add("has-mood", Json.createArrayBuilder()
                        .add(Json.createObjectBuilder()
                            .add("roles", Json.createObjectBuilder()
                                .add("mood-of-production", concept)
                                .add("production-with-mood", movie)
                            )
                        )
                    )
                )
            )
            .build();
    }
}
