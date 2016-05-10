import java.util.Map;
import java.util.Set;

/**
 *
 */

public class FixKeywords {
    LoadExtractedData data;

    public FixKeywords (LoadExtractedData d) {data = d;}

    public void prepareIIDMaps(){
        data.resolutions.forEach((k,v)->{
            Set<String> iids = data.conceptsIID.get("keyword").get(k);
            if (iids.size()==1) {
                System.out.print(iids.iterator().next()+"-->");
            }
            for (String value : v) {
                Set<String> vIIDS = data.conceptsIID.get(value).get(k);
                if (vIIDS.size()==1) {
                    System.out.println(vIIDS.iterator().next());
                } else {
                    System.out.println("");
                }
            }
            System.out.println("");
        });
    }
}
