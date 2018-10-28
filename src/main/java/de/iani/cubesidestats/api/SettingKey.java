package de.iani.cubesidestats.api;

public interface SettingKey {
    public String getName();

    public void setDisplayName(String name);

    public String getDisplayName();

    public int getDefault();

    public void setDefault(int def);
}
