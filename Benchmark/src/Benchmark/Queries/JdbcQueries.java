package Benchmark.Queries;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class JdbcQueries implements Queries {
    protected Connection connection;

    @Override
    public void done() throws SQLException {
        connection.close();
    }
}
