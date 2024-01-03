package app.config;

import config.Config;

@Config
public class HamtConfig {
    public boolean useHAMT;
    public int activeThreshold;
    public boolean useCustomHAMTSettings;
    public CustomHAMTSettingsConfig customHAMTSettings;
}
