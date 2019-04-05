package simpledb.buffer;

import simpledb.remote.SimpleDriver;

import java.sql.*;

public class QueryTest {
    public static void main(String[] args) {
        Connection connection = null;
        try {
            Driver driver = new SimpleDriver();
            connection = driver.connect("jdbc:simpledb://localhost", null);

            long startTime = System.currentTimeMillis();

            //Query for retrieving those with SId == 5 (should not be in buffer, replace LRU, then get pinned, then unpinned)
            System.out.println("Student with SId = 5");
            Statement statement = connection.createStatement();
            String sqlQuery = "SELECT SName from STUDENT WHERE SId = 5";

            ResultSet resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("SName");
                System.out.println(name);
            }
            resultSet.close();

            //Query for retrieving those with SId == 5 (should now be found in buffer, get pinned, then unpinned)
            System.out.println("All students with SId = 5");
            statement = connection.createStatement();
            sqlQuery = "SELECT SName from STUDENT WHERE SId = 5";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("SName");
                System.out.println(name);
            }
            resultSet.close();

            //Insert new test student to modify buffer
            System.out.println("Inserting new test student with SId = 99");
            String s = "insert into STUDENT(SId, SName, MajorId, GradYear) values ";
            String testStudent = "(99, 'bill', 10, 9999)";
            statement.executeUpdate(s + testStudent);
            System.out.println("Test STUDENT record inserted.");

            //Query for retrieving those with SId == 99 (should have to be added to buffer)
            System.out.println("All students with SId = 99");
            statement = connection.createStatement();
            sqlQuery = "SELECT SName from STUDENT WHERE SId = 99";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("SName");
                System.out.println(name);
            }
            resultSet.close();

            long endTime = System.currentTimeMillis();

            System.out.println("Time passed in milliseconds: " + (endTime - startTime));
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
