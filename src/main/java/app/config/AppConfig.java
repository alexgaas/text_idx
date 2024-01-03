package app.config;

import config.Config;

@Config
public class AppConfig {
    public String sourceDir;
    public String indexDir;
    public int scan;
    public HamtConfig hamt;
}
