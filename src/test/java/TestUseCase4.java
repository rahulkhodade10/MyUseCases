import org.junit.Assert;
import org.junit.Test;

public class TestUseCase4 {


    public static long getOrderCount()
    {
        long orderCount=UseCasesRowData.getOrders().count();
        return orderCount;
    }

    public static long getOrder_items_Count()
    {
        long order_items_count=UseCasesRowData.getOrder_items().count();
        return order_items_count;
    }

    public static long getCategories()
    {
        long categoriesCount=UseCasesRowData.getCategories().count();
        return  categoriesCount;
    }
     public static long getProducts()
     {
         long productCount=UseCasesRowData.getProducts().count();
         return productCount;
     }

     public static long getResultCount()
     {
         long resultCount=UseCase4.getUseCase4Result().count();
         return resultCount;
     }


    @Test

    public void validateOrders()
    {
        Assert.assertEquals(68883,getOrderCount());
    }
    @Test
    public void validateOrder_Item()
    {
        Assert.assertEquals(172198,getOrder_items_Count());
    }

   @Test
    public void validateCategories()
   {
       Assert.assertEquals(58,getCategories());
   }

   @Test
    public void validateProducts()
   {
       Assert.assertEquals(1345,getProducts());
   }

   @Test
    public void validResult()
   {
       Assert.assertEquals(33,getResultCount());
   }
}
