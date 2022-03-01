import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.xml.crypto.Data;

public class UseCasesRowData {

    public static Dataset<Row> getOrders()
    {
        String orders_path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\MyUseCases\\src\\main\\resources\\retail_db\\orders\\part-00000";
        Dataset<Row> orders = GetSession.getSparkSession().read().format("csv").option("header", "true").option("inferSchema", "true").load(orders_path);

        return orders;
    }

    public static Dataset<Row> getCustomers()
    {
        String customers_path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\MyUseCases\\src\\main\\resources\\retail_db\\customers\\part-00000";
        Dataset<Row> customers = GetSession.getSparkSession().read().format("csv").option("header", "true").option("inferSchema", "true").load(customers_path);

        return customers;
    }

    public static Dataset<Row> getOrder_items()
    {
        String order_items_path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\order_items\\part-00000";
        Dataset<Row> order_items=GetSession.getSparkSession().read().format("csv").option("header","true").option("inferSchema","true").load(order_items_path);

        return order_items;
    }
    public static Dataset<Row> getProducts()
    {
        String products_path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\products\\part-00000";
        Dataset<Row> products=GetSession.getSparkSession().read().format("csv").option("header","true").option("inferSchema","true").load(products_path);

        return products;
    }
    public static Dataset<Row> getCategories()
    {
        String categories_path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\categories\\part-00000";
        Dataset<Row> categories=GetSession.getSparkSession().read().format("csv").option("header","true").option("inferSchema","true").load(categories_path);

        return categories;
    }
   public static Dataset<Row> getDepartments()
   {
       String departments_path = "C:\\Users\\Rahul Khodade\\IdeaProjects\\UseCases\\src\\main\\resources\\retail_db\\departments\\part-00000";
       Dataset<Row> departments=GetSession.getSparkSession().read().format("csv").option("header","true").option("inferSchema","true").load(departments_path);

       return departments;
   }

}
