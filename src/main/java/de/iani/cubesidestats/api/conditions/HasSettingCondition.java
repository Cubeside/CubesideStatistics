package de.iani.cubesidestats.api.conditions;

import de.iani.cubesidestats.api.CubesideStatisticsAPI;
import de.iani.cubesidestats.api.SettingKey;
import de.iani.cubesideutils.conditions.Condition;
import java.util.Objects;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

public class HasSettingCondition implements Condition<Player> {

    public static final String SERIALIZATION_TYPE = "StatisticsHasSettingCondition";
    private static CubesideStatisticsAPI cubesideStatisticsAPI;

    public static HasSettingCondition deserialize(String serialized) {
        String[] parts = serialized.split("\\,");
        return new HasSettingCondition(Condition.unescape(parts[0]), Integer.parseInt(parts[1]), CompareOperation.valueOf(parts[2]));
    }

    private String settingKey;
    private int compare;
    private CompareOperation op;

    public HasSettingCondition(String settingKey, int compare, CompareOperation op) {
        this.settingKey = Objects.requireNonNull(settingKey);
        this.compare = compare;
        this.op = Objects.requireNonNull(op);
    }

    @Override
    public boolean test(Player t) {
        SettingKey key = getCubesideStatisticsAPI().getSettingKey(settingKey, false);
        if (key == null) {
            return false;
        }
        int value = getCubesideStatisticsAPI().getStatistics(t.getUniqueId()).getSettingValueOrDefault(key);
        return op.apply(value, compare);
    }

    @Override
    public String getSerializationType() {
        return SERIALIZATION_TYPE;
    }

    @Override
    public String serializeToString() {
        return Condition.escape(settingKey) + "," + compare + "," + op.name();
    }

    private static CubesideStatisticsAPI getCubesideStatisticsAPI() {
        CubesideStatisticsAPI local = cubesideStatisticsAPI;
        if (local == null) {
            local = Bukkit.getServer().getServicesManager().load(CubesideStatisticsAPI.class);
            cubesideStatisticsAPI = local;
        }
        return local;
    }

    public enum CompareOperation {
        EQUAL {
            @Override
            public boolean apply(int a, int b) {
                return a == b;
            }
        },
        NOT_EQUAL {
            @Override
            public boolean apply(int a, int b) {
                return a != b;
            }
        },
        LESS {
            @Override
            public boolean apply(int a, int b) {
                return a < b;
            }
        },
        LESS_OR_EQUAL {
            @Override
            public boolean apply(int a, int b) {
                return a <= b;
            }
        },
        GREATER {
            @Override
            public boolean apply(int a, int b) {
                return a > b;
            }
        },
        GREATER_OR_EQUAL {
            @Override
            public boolean apply(int a, int b) {
                return a >= b;
            }
        };

        public abstract boolean apply(int a, int b);
    }
}
