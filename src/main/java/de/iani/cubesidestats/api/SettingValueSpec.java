package de.iani.cubesidestats.api;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Describes the values that are valid for a setting and, optionally, their
 * semantic identifiers.
 *
 * <p>The bounds are inclusive. A setting without an explicit restriction can
 * use {@link #unrestricted()}, which accepts every {@code int} value.</p>
 */
public final class SettingValueSpec {
    private final int minimum;
    private final int maximum;
    private final Map<Integer, String> meanings;

    private SettingValueSpec(int minimum, int maximum, Map<Integer, String> meanings) {
        if (minimum > maximum) {
            throw new IllegalArgumentException("minimum must not be greater than maximum");
        }

        LinkedHashMap<Integer, String> copy = new LinkedHashMap<>();
        for (Map.Entry<Integer, String> entry : meanings.entrySet()) {
            Integer value = Objects.requireNonNull(entry.getKey(), "meaning value");
            String meaning = Objects.requireNonNull(entry.getValue(), "meaning");
            if (meaning.isBlank()) {
                throw new IllegalArgumentException("meaning must not be blank");
            }
            if (value < minimum || value > maximum) {
                throw new IllegalArgumentException("meaning value is outside the allowed range: " + value);
            }
            if (copy.containsValue(meaning)) {
                throw new IllegalArgumentException("meaning must be unique: " + meaning);
            }
            copy.put(value, meaning);
        }

        this.minimum = minimum;
        this.maximum = maximum;
        this.meanings = Collections.unmodifiableMap(copy);
    }

    /**
     * Creates a specification that accepts every {@code int} value.
     *
     * @return an unrestricted value specification
     */
    public static SettingValueSpec unrestricted() {
        return new SettingValueSpec(Integer.MIN_VALUE, Integer.MAX_VALUE, Collections.emptyMap());
    }

    /**
     * Creates a specification with inclusive lower and upper bounds.
     *
     * @param minimum the smallest allowed value
     * @param maximum the greatest allowed value
     * @return a value specification
     */
    public static SettingValueSpec range(int minimum, int maximum) {
        return new SettingValueSpec(minimum, maximum, Collections.emptyMap());
    }

    /**
     * Creates a specification from a range and its semantic identifiers.
     *
     * @param minimum the smallest allowed value
     * @param maximum the greatest allowed value
     * @param meanings mappings from numeric values to semantic identifiers
     * @return a value specification
     */
    public static SettingValueSpec of(int minimum, int maximum, Map<Integer, String> meanings) {
        return new SettingValueSpec(minimum, maximum, Objects.requireNonNull(meanings, "meanings"));
    }

    /**
     * Returns a copy with a semantic identifier assigned to a value.
     *
     * @param value the value to describe
     * @param meaning the stable semantic identifier
     * @return the updated value specification
     */
    public SettingValueSpec withMeaning(int value, String meaning) {
        LinkedHashMap<Integer, String> copy = new LinkedHashMap<>(meanings);
        copy.remove(value);
        copy.put(value, meaning);
        return new SettingValueSpec(minimum, maximum, copy);
    }

    /**
     * Returns a copy without a semantic identifier for the given value.
     *
     * @param value the value whose meaning should be removed
     * @return the updated value specification
     */
    public SettingValueSpec withoutMeaning(int value) {
        if (!meanings.containsKey(value)) {
            return this;
        }
        LinkedHashMap<Integer, String> copy = new LinkedHashMap<>(meanings);
        copy.remove(value);
        return new SettingValueSpec(minimum, maximum, copy);
    }

    public int getMinimum() {
        return minimum;
    }

    public int getMaximum() {
        return maximum;
    }

    public boolean isAllowed(int value) {
        return value >= minimum && value <= maximum;
    }

    public Optional<String> getMeaning(int value) {
        return Optional.ofNullable(meanings.get(value));
    }

    public OptionalInt getValue(String meaning) {
        if (meaning == null) {
            return OptionalInt.empty();
        }
        for (Map.Entry<Integer, String> entry : meanings.entrySet()) {
            if (entry.getValue().equals(meaning)) {
                return OptionalInt.of(entry.getKey());
            }
        }
        return OptionalInt.empty();
    }

    public Map<Integer, String> getMeanings() {
        return meanings;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SettingValueSpec other)) {
            return false;
        }
        return minimum == other.minimum && maximum == other.maximum && meanings.equals(other.meanings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimum, maximum, meanings);
    }

    @Override
    public String toString() {
        return "SettingValueSpec{" + "minimum=" + minimum + ", maximum=" + maximum + ", meanings=" + meanings + '}';
    }
}
