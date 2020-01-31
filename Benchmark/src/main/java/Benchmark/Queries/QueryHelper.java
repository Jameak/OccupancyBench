package Benchmark.Queries;

import Benchmark.Generator.GeneratedData.AccessPoint;
import Benchmark.Generator.GeneratedData.Floor;

/**
 * Contains static helper functions used by multiple query-implementations.
 */
public class QueryHelper {
    public static String buildRowSchemaFloorTotalQueryPrecomputation(AccessPoint[] APs){
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        boolean first = true;
        for(AccessPoint AP : APs){
            if(first){
                first = false;
            } else {
                sb.append("OR");
            }
            sb.append(" AP='");
            sb.append(AP.getAPname());
            sb.append("' ");
        }
        sb.append(")");
        return sb.toString();
    }

    public static String buildColumnSchemaTotalClientsQueryPrecomputation(AccessPoint[] APs){
        StringBuilder builder = new StringBuilder("(");
        createAddedSumStatements(builder, APs);
        builder.append(") as total");
        return builder.toString();
    }

    public static String buildColumnSchemaFloorTotalQueryPrecomputation(Floor[] generatedFloors){
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < generatedFloors.length; i++) {
            if(i != 0){
                builder.append(",");
            }

            Floor floor = generatedFloors[i];
            builder.append("(");
            createAddedSumStatements(builder, floor.getAPs());
            builder.append(") as floor");
            builder.append(floor.getFloorNumber());
        }
        return builder.toString();
    }

    private static void createAddedSumStatements(StringBuilder builder, AccessPoint[] APs){
        for (int i = 0; i < APs.length; i++) {
            if (i != 0) {
                builder.append(" + ");
            }
            builder.append("SUM");
            builder.append("(\"");
            AccessPoint AP = APs[i];
            builder.append(AP.getAPname());
            builder.append("\")");
        }
    }

    public static String buildColumnSchemaAvgOccupancyPrecomputation_AVG(String avgOperator, AccessPoint[] APs){
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < APs.length; i++) {
            if (i != 0) {
                builder.append(",");
            }
            // Influx uses 'MEAN' and TimescaleDB uses 'AVG'
            builder.append(avgOperator);
            builder.append("(\"");
            AccessPoint AP = APs[i];
            builder.append(AP.getAPname());
            builder.append("\") as \"avg-");
            builder.append(AP.getAPname());
            builder.append("\"");
        }
        return builder.toString();
    }

    public static String buildColumnSchemaAvgOccupancyPrecomputation_SELECT_ALL(AccessPoint[] APs){
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < APs.length; i++) {
            if (i != 0) {
                builder.append(",");
            }
            builder.append("\"");
            AccessPoint AP = APs[i];
            builder.append(AP.getAPname());
            builder.append("\"");
        }
        return builder.toString();
    }
}
