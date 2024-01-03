package config;

import app.config.AppConfig;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConfigParserTest {
    /*
    # dir with text files to index for
    sourceDir: ./resources

    # dir to save index
    indexDir: ./idx

    # full scan algorithm
    # 0 - Wu-Manber
    # 1 - Harspool
    scan: 0

    # HashMappedArrayTree settings
    hamt:
      # use hamt in memory. if false only btree is going to be used to store index
      useHAMT: true
      # active threshold defines number of operations after we start to put data in btree in parallel
      activeThreshold: 5000
      # defines custom settings for HAMT, false by default
      useCustomHAMTSettings: false
      customHAMTSettings:
        # size of hashmap, that WON'T be resized when we reach fulfill (no fill factor)
        hashMapSize: 64
     */
    private static final String TEST_CONFIG = "\n" +
            "sourceDir: ./resources\n" +
            "indexDir: ./idx\n" +
            "scan: 0\n" +
            "hamt:\n" +
            "   useHAMT: true\n" +
            "   activeThreshold: 5000\n" +
            "   useCustomHAMTSettings: false\n" +
            "   customHAMTSettings:\n" +
            "       hashMapSize: 64\n";


    @Test
    public void testConfigParser() throws Exception {
        AppConfig config = ConfigParser.parse(TEST_CONFIG, AppConfig.class);

        assertEquals("./resources", config.sourceDir);
        assertEquals("./idx", config.indexDir);
        assertEquals(0, config.scan);

        assertEquals(true, config.hamt.useHAMT);
        assertEquals(5000, config.hamt.activeThreshold);
        assertEquals(false, config.hamt.useCustomHAMTSettings);
        assertEquals(64, config.hamt.customHAMTSettings.hashMapSize);
    }
}
