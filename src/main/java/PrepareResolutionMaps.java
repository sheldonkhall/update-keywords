import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 */

public class PrepareResolutionMaps {
    LoadExtractedData data;
    Map<String,String> resolutionIIDMap;
    Map<String,String> resolutionRelationType;

    public PrepareResolutionMaps(LoadExtractedData d) {
        data = d;
    }

    public void prepareMaps(){
        // create the map for keywords to be updated
        resolutionIIDMap = new HashMap<>();
        resolutionRelationType = new HashMap<>();
        data.resolutions.forEach((k,v)->{
            // skip if there are no resolution specified
            if (v.size()>0) {
                Set<String> iids = data.conceptsIID.get("keyword").get(k);
                // ensure not null or ambiguous
                if (iids != null) {
                    if (iids.size() == 1) {
                        // store iids for resolutions
                        String keywordIID = iids.iterator().next();
                        for (String value : v) {
                            Set<String> replacementIIDS = data.conceptsIID.get(value).get(k);
                            if (replacementIIDS != null) {
                                if (replacementIIDS.size() == 1) {
                                    // record iid
                                    resolutionIIDMap.put(keywordIID, replacementIIDS.iterator().next());
                                    // record relation type
                                    resolutionRelationType.put(keywordIID, value);
                                } else {
                                    System.out.println(value + " - " + k + " - found with multiple iids");
                                }
                            } else {
                                System.out.println(value + " - " + k + " - found with null iid");
                            }
                        }
                    } else {
                        System.out.println("Skipping - " + k + " - because 0 or more than 1 iid exists - " + iids);
                    }
                } else {
                    System.out.println("Null iid for keyword - " + k);
                }
            } else {
                System.out.println("keyword - "+k+" - has no resolutions, skipping...");
            }
        });
    }
}
