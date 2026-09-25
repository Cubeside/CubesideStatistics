package de.iani.cubesidestats.api;

public interface SettingKey {
    public String getName();

    public void setDisplayName(String name);

    public String getDisplayName();

    public int getDefault();

    public void setDefault(int def);

    /**
     * Returns the value range and semantic identifiers for this setting.
     *
     * <p>The default implementation keeps older third-party implementations
     * unrestricted. Setting keys returned by the CubesideStatistics API
     * support changing the specification.</p>
     *
     * @return the value specification
     */
    public default SettingValueSpec getValueSpec() {
        return SettingValueSpec.unrestricted();
    }

    /**
     * Sets the value range and semantic identifiers for this setting.
     *
     * @param valueSpec the new value specification
     */
    public default void setValueSpec(SettingValueSpec valueSpec) {
        throw new UnsupportedOperationException("This setting key does not support value specifications");
    }

    /**
     * Checks whether a value is valid for this setting.
     *
     * @param value the value to check
     * @return whether the value is within the configured range
     */
    public default boolean isValueAllowed(int value) {
        return getValueSpec().isAllowed(value);
    }
}
