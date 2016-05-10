/**
 *
 */

public class main {
    public static void main(String [ ] args) {
        LoadExtractedData data = new LoadExtractedData();
        data.loadFromDisk();
        FixKeywords fix = new FixKeywords(data);
        fix.prepareIIDMaps();

        System.exit(0);
    }
}
