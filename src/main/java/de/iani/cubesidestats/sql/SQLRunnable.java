package de.iani.cubesidestats.sql;

import java.sql.Connection;
import java.sql.SQLException;

public interface SQLRunnable<T>
{
    public T execute(Connection connection, SQLConnection sqlConnection) throws SQLException;
}
