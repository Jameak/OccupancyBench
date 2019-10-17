package Benchmark.Generator.Targets;


import java.sql.Connection;

public abstract class JdbcTarget implements ITarget {
    protected Connection connection;
    protected boolean error;

    @Override
    public boolean shouldStopEarly(){
        return error;
    }
}
