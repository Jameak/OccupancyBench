package Benchmark.Databases.Timescale;

import Benchmark.Generator.GeneratedData.AccessPoint;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Contains static helper functions used by the Timescale-database implementations.
 */
public class TimescaleHelper {
    /**
     * Opens a connection to the Timescale database.
     */
    public static Connection openConnection(String username, String password, String host, String dbname, boolean reWriteBatchedInserts) throws SQLException {
        DriverManager.registerDriver(new org.postgresql.Driver());
        // TODO: Include SSL-parameter in the connection string?
        String db;
        if(reWriteBatchedInserts){
            db = String.format("jdbc:postgresql://%s/%s?reWriteBatchedInserts=true", host, dbname);
        } else {
            db = String.format("jdbc:postgresql://%s/%s",host, dbname);
        }
        return DriverManager.getConnection(db, username, password);
    }

    /**
     * Drops the specified table from the database.
     */
    public static void dropTable(Connection connection, String table) throws SQLException {
        String query1 = String.format("DROP TABLE %s", table);

        try(Statement statement = connection.createStatement()){
            try{
                statement.executeUpdate(query1);
            } catch (SQLException e){
                // Table didn't exist.
            }
        }
    }

    /**
     * Creates the specified table in the database.
     * The table is then made into a hypertable (so that the Timescale-extension is used rather than plain PostgreSQL)
     * Then an index is created on the table.
     */
    public static void createTableWithRowSchema(Connection connection, String table) throws SQLException {
        String query1 = String.format(
                "CREATE TABLE %s (" +
                "time    TIMESTAMP NOT NULL," +
                "AP      TEXT      NOT NULL," +
                "clients INTEGER   NOT NULL)", table);

        String query2 = String.format("SELECT create_hypertable('%s', 'time')", table);

        String query3 = String.format("CREATE INDEX ON %s (AP, time DESC)", table);

        try(Statement statement = connection.createStatement()){
            statement.executeUpdate(query1);
            statement.execute(query2);
            statement.executeUpdate(query3);
        }
    }

    /**
     * Creates the specified table in the database.
     * The table is then made into a hypertable (so that the Timescale-extension is used rather than plain PostgreSQL)
     */
    public static void createTableWithColumnSchema(Connection connection, String table, AccessPoint[] allAPs) throws SQLException {
        assert allAPs.length > 0 : "Making table for access points without any access points makes no sense";

        StringBuilder sb = new StringBuilder(String.format(
                "CREATE TABLE %s (" +
                "time   TIMESTAMP NOT NULL", table));
        for(AccessPoint AP : allAPs){
            sb.append(",\"");
            //AP-names contain a '-' character that needs to be escaped
            sb.append(AP.getAPname());
            sb.append("\" INTEGER NOT NULL");
        }
        sb.append(")");

        String query1 = sb.toString();
        String query2 = String.format("SELECT create_hypertable('%s', 'time')", table);

        try(Statement statement = connection.createStatement()){
            statement.executeUpdate(query1);
            statement.execute(query2);
        }
    }
}
