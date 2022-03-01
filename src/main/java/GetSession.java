
import org.apache.spark.sql.SparkSession;

public class GetSession {

    public static SparkSession getSparkSession()
    {
        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        return spark;
    }
}
