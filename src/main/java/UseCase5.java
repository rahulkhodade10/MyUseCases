import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.count;

public class UseCase5 {

    public static Dataset<Row> getUseCase5Result()
    {
        Dataset<Row> departments=UseCasesRowData.getDepartments();
        Dataset<Row> products=UseCasesRowData.getProducts();
        Dataset<Row> categories=UseCasesRowData.getCategories();

        Dataset<Row> join3 = departments.join(categories, departments.col("department_id").equalTo(categories.col("category_department_id")));
        Dataset<Row> finalrov1 = join3.join(products, join3.col("category_id").equalTo(products.col("product_category_id"))).
                groupBy(join3.col("department_id"),
                        join3.col("department_name")).
                agg(count(products.col("product_category_id"))).
                orderBy(join3.col("department_id"));

        return finalrov1;
    }


    public static void main(String[] args) {


        String path="C:\\Users\\Rahul Khodade\\IdeaProjects\\MyUseCases\\src\\main\\outputs\\useCase5";
        getUseCase5Result().coalesce(1).write().option("header",true).mode("overwrite").csv(path);




    }


}
