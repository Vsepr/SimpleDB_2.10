package simpledb.buffer;

import simpledb.remote.SimpleDriver;

import java.sql.*;

public class Examples {
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

            //Query for getting departments
            System.out.println("Department with DId > 0");
            statement = connection.createStatement();
            sqlQuery = "SELECT DName from DEPT HAVING DId > 0";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("DName");
                System.out.println(name);
            }
            resultSet.close();

            //Query for getting courses with DeptId = 10
            System.out.println("Courses in compsi major");
            statement = connection.createStatement();
            sqlQuery = "SELECT Title from COURSE WHERE DeptId = 10";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("Title");
                System.out.println(name);
            }
            resultSet.close();

            //Query for courses in math dept with CId = 32
            System.out.println("Retrieve courses in math dept with CId = 32");
            statement = connection.createStatement();
            sqlQuery = "SELECT Title from COURSE WHERE DeptId = 20 AND CId = 32";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("Title");
                System.out.println(name);
            }
            resultSet.close();

            //Query for DB and acting courses
            System.out.println("Retrieve db systems & acting courses");
            statement = connection.createStatement();
            sqlQuery = "SELECT Title from COURSE WHERE Title = 'db systems' OR Title = 'acting'";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("Title");
                System.out.println(name);
            }
            resultSet.close();

            // test creating a new table
            System.out.println("Create new table \'TEST\'");
            s = "create table TEST(Num int, Nam varchar(20))";
            statement.executeUpdate(s);
            System.out.println("Table TEST created.");

            // insert into new table
            s = "insert into TEST(Num, Nam) values ";
            String[] newTests = {"(90, 'test90')",
                    "(80, 'test80')",
                    "(70, 'test70')",
                    "(60, 'test60')",
                    "(50, 'test50')",
                    "(40, 'test40')"};

            for (int i=0; i<newTests.length; i++)
                statement.executeUpdate(s + newTests[i]);
            System.out.println("COURSE records inserted.");

            // tests querying new table
            System.out.println("Retrieve test90");
            statement = connection.createStatement();
            sqlQuery = "SELECT Nam from TEST WHERE Num = 90";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("Nam");
                System.out.println(name);
            }
            resultSet.close();

            System.out.println("Retrieve all files in table with Num > 0");
            statement = connection.createStatement();
            sqlQuery = "SELECT Nam from TEST HAVING Num > 0";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("Nam");
                System.out.println(name);
            }
            resultSet.close();

            System.out.println("Retrieve test80 by Nam & Num");
            statement = connection.createStatement();
            sqlQuery = "SELECT Nam from TEST WHERE Num = 80 AND Nam = 'test80'";

            resultSet = statement.executeQuery(sqlQuery);

            while(resultSet.next()) {
                String name = resultSet.getString("Nam");
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
