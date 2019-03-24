package de.iani.cubesidestats;

import de.iani.cubesidestats.CubesideStatisticsImplementation.WorkEntry;
import de.iani.cubesidestats.api.GlobalStatisticKey;
import java.sql.SQLException;
import java.util.logging.Level;

public class GlobalStatisticKeyImplementation extends StatisticKeyImplementationBase implements GlobalStatisticKey {

    public GlobalStatisticKeyImplementation(int id, String name, String properties, CubesideStatisticsImplementation impl) {
        super(id, name, properties, impl);
    }

    @Override
    protected void save() {
        GlobalStatisticKeyImplementation clone = new GlobalStatisticKeyImplementation(id, name, null, stats);
        clone.copyPropertiesFrom(this);

        stats.getWorkerThread().addWork(new WorkEntry() {
            @Override
            public void process(StatisticsDatabase database) {
                try {
                    database.updateGlobalStatisticKey(clone);
                } catch (SQLException e) {
                    stats.getPlugin().getLogger().log(Level.SEVERE, "Could not save global statistic key " + name, e);
                }
            }
        });
    }
}
