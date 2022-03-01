import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class UseCase4 {

    public static Dataset<Row> getUseCase4Result()
    {
        Dataset<Row> orders=UseCasesRowData.getOrders();
        Dataset<Row> order_items=UseCasesRowData.getOrder_items();
        Dataset<Row> products=UseCasesRowData.getProducts();
        Dataset<Row> categories=UseCasesRowData.getCategories();

        Dataset<Row> join1 = orders.join(order_items, orders.col("order_id").equalTo(order_items.col("order_item_order_id")));
        Dataset<Row> join2 = products.join(join1,products.col("product_id").equalTo(join1.col("order_item_product_id")));
        Dataset<Row> RevenuePerCategory = join2.join(categories, join2.col("product_category_id").equalTo(categories.col("category_id"))).
                where(orders.col("order_date").like("2014-01%").and(join2.col("order_status").isin("COMPLETE", "CLOSED"))).
                groupBy(categories.col("category_id"),
                        categories.col("category_department_id"),
                        categories.col("category_name")).
                agg(round(sum(join2.col("order_item_subtotal")), 2).alias("category_revenue")).
                orderBy(categories.col("category_id"));

        return RevenuePerCategory;

    }

    public static void main(String[] args) {

        String path="C:\\Users\\Rahul Khodade\\IdeaProjects\\MyUseCases\\src\\main\\outputs\\useCase4";
        getUseCase4Result().coalesce(1).write().option("header",true).mode("overwrite").csv(path);





    }

}

