package simpledb.buffer;

import simpledb.remote.SimpleDriver;

import java.sql.*;

public class TestQueries {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            Driver driver = new SimpleDriver();
            connection = driver.connect("jdbc:simpledb://localhost", null);

            //Query for retrieving those with SId > 5 (should be sue, bob, kim, art, pat, lee)
            System.out.println("All students with SId > 3");
            Statement statement = connection.createStatement();
            String sqlQuery = "SELECT SName from STUDENT HAVING SId > 3";

            ResultSet resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("SName");
                System.out.println(name);
            }
            resultSet.close();

            //Query for retrieving those with SId > 5 (should be kim, art, pat, lee)
            System.out.println("All students with SId > 5");
            sqlQuery = "SELECT SName from STUDENT HAVING SId > 5";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("SName");
                System.out.println(name);
            }
            resultSet.close();

            //Query for retrieving those with name not Joe (should be kim, art, pat, lee)
            System.out.println("All students with name that isn't joe");
            sqlQuery = "SELECT SName from STUDENT HAVING SName NOT 'joe'";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("SName");
                System.out.println(name);
            }
            resultSet.close();

            // This should fill the buffer, now querying for Joe should cause an unpin and he should get pinned
            System.out.println("All students with name that is joe");
            sqlQuery = "SELECT SName from STUDENT HAVING SName == 'joe'";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("SName");
                System.out.println(name);
            }
            resultSet.close();


            /*
            String s = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)";
            stmt.executeUpdate(s);
            System.out.println("Table STUDENT created.");

            s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
            String[] studvals = {"(1, 'joe', 10, 2004)",
                    "(2, 'amy', 20, 2004)",
                    "(3, 'max', 10, 2005)",
                    "(4, 'sue', 20, 2005)",
                    "(5, 'bob', 30, 2003)",
                    "(6, 'kim', 20, 2001)",
                    "(7, 'art', 30, 2004)",
                    "(8, 'pat', 20, 2001)",
                    "(9, 'lee', 10, 2004)"};
            */
        }
        catch (Exception exception) {
            exception.printStackTrace();
        }

        try {
            if(connection != null) {
                connection.close();
            }
        }
        catch (SQLException exception) {
            exception.printStackTrace();
        }
    }
}
