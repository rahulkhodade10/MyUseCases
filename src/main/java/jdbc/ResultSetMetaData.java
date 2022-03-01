//package jdbc;
//import java.sql.*;
//
//public class ResultSetMetaData {
//
//    public static void main(String[] args) {
//        try {
//            Class.forName("oracle.jdbc.driver.OracleDriver");
//            Connection con=DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","system","oracle");
//            PreparedStatement ps=con.prepareStatement("select * from emp");
//            ResultSet rs=ps.executeQuery();
//            ResultSetMetaData rsmd= (ResultSetMetaData) rs.getMetaData();
//            System.out.println("Total columns: "+rsmd.getColumnCount());
//            System.out.println("Column Name of 1st column: "+rsmd.getColumnName(1));
//            System.out.println("Column Type Name of 1st column: "+rsmd.getColumnTypeName(1));
//
//            con.close();
//
//        }
//        catch (Exception e)
//
//        {
//            System.out.println(e);
//        }
//    }
//
//}
