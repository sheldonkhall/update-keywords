import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Retrieve data extracted from a graph to use in conjunction with a resolution file.
 */

public class LoadExtractedData {
    public Set<String> conceptNames = new HashSet<>();
    public Map<String,Map<String,Set<String>>> conceptsIID = new HashMap<>();
    public Map<String,Map<String,Long>> conceptsDegree = new HashMap<>();
    public Map<String,Set<String>> resolutions = new HashMap<>();

    public void loadFromDisk() {
        loadIID();
        loadDegree();
        loadResolutions();
    }

    private void loadIID() {
        System.out.println("Reading IIDs from disk");
        File conceptIIDPath = new File("raw");
        for (File conceptIIDFile : conceptIIDPath.listFiles((x, n) -> !n.endsWith("Degrees.txt"))) {
            String conceptName = conceptIIDFile.getName().toString().replace(".txt", "");
            conceptNames.add(conceptName);
            conceptsIID.put(conceptName,new HashMap<String,Set<String>>());
            try (BufferedReader reader = Files.newBufferedReader(conceptIIDFile.toPath())) {
                String line;
                while ((line = reader.readLine())!=null) {
                    String[] lineParts = line.split(";");
                    lineParts[1] = lineParts[1].replace("[","").replace("]","");
                    String[] iids = lineParts[1].split(",");
                    if (conceptsIID.get(conceptName).containsKey(lineParts[0])) {
                        for (String iid : iids) {
                            conceptsIID.get(conceptName).get(lineParts[0]).add(iid.trim());
                        }
                    } else {
                        conceptsIID.get(conceptName).put(lineParts[0],new HashSet<String>());
                        for (String iid : iids) {
                            conceptsIID.get(conceptName).get(lineParts[0]).add(iid.trim());
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Finished Loading IIDs from disk");
    }

    private void loadDegree() {
        System.out.println("Reading Degrees from disk");
        File conceptIIDPath = new File("raw");
        for (File conceptIIDFile : conceptIIDPath.listFiles((x, n) -> n.endsWith("Degrees.txt"))) {
            String conceptName = conceptIIDFile.getName().toString().replace("Degrees.txt", "");
            conceptsDegree.put(conceptName,new HashMap<String,Long>());
            try (BufferedReader reader = Files.newBufferedReader(conceptIIDFile.toPath())) {
                String line;
                while ((line = reader.readLine())!=null) {
                    String[] lineParts = line.split(";");
                    conceptsDegree.get(conceptName).put(lineParts[0],Long.valueOf(lineParts[1]));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Finished Loading Degrees from disk");
    }

    private void loadResolutions() {
        System.out.println("Reading Resolutions from disk");
        File resolutionsPath = new File("finalResolution.txt");
        try (BufferedReader reader = Files.newBufferedReader(resolutionsPath.toPath())) {
            String line;
            while ((line = reader.readLine())!=null) {
                String[] lineParts = line.split(";");
                if (!resolutions.containsKey(lineParts[0])) {
                    resolutions.put(lineParts[0],new HashSet<String>());
                }
                if (lineParts.length > 1){
                    for (int i=1; i<lineParts.length;i++) {
                        resolutions.get(lineParts[0]).add(lineParts[i].trim());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Finished Loading Resolutions from disk");
    }
}
