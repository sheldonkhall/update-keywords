/**
 *
 */

public class main {
    public static void main(String [ ] args) {
        LoadExtractedData data = new LoadExtractedData();
        data.loadFromDisk();
        FixKeywords fix = new FixKeywords(data);
        fix.prepareIIDMaps();
        fix.resolutionIIDMap.forEach((k,v)->fix.updateKeyword(k,v));
        System.exit(0);
    }
}
