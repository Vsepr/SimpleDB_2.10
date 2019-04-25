import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import simpledb.remote.SimpleDriver;
public class CreateTestTables {
    final static int maxSize=20000;
    /**
     * @param args
     */

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Connection conn=null;
        Driver d = new SimpleDriver();
        String host = "localhost"; //you may change it if your SimpleDB server is running on a different machine
        String url = "jdbc:simpledb://" + host;
        String qry="Create table test1" +
                "( a1 int," +
                "  a2 int"+
                ")";
        Random rand=null;
        Statement s=null;
        try {
            conn = d.connect(url, null);
            s=conn.createStatement();

            System.out.println("Creating tables...");
            s.executeUpdate("Create table test1" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test2" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test3" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test4" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");
            s.executeUpdate("Create table test5" +
                    "( a1 int," +
                    "  a2 int"+
                    ")");

            System.out.println("Creating indecies...");
            s.executeUpdate("create sh index idx1 on test1 (a1)");
            s.executeUpdate("create eh index idx2 on test2 (a1)");
            s.executeUpdate("create bt index idx3 on test3 (a1)");

            System.out.println("Inserting data...");
            for(int i=1;i<6;i++)
            {
                if(i!=5)
                {
                    rand=new Random(1);// ensure every table gets the same data
                    for(int j=0;j<maxSize;j++)
                    {
                        s.executeUpdate("insert into test"+i+" (a1,a2) values("+rand.nextInt(1000)+","+rand.nextInt(1000)+ ")");
                    }
                }
                else//case where i=5
                {
                    for(int j=0;j<maxSize/2;j++)// insert 10000 records into test5
                    {
                        s.executeUpdate("insert into test"+i+" (a1,a2) values("+j+","+j+ ")");
                    }
                }
            }

            //TESTING

            //first run SELECT tests
            //test 1, select on one attribute
            System.out.println("\nRunning SELECT tests...\n");

            long start = System.nanoTime();
            String query = "select a1, a2 from test1 where a1 = 1";
            s.executeQuery(query);
            long end = System.nanoTime();
            long totalTime1 = end - start;
            System.out.println("Time for static hash:        "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test2 where a1 = 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for extensible hash:    "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test3 where a1 = 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for b-tree:             "+totalTime1);

            //test 2, select on two attributes
            System.out.println();
            start = System.nanoTime();
            query = "select a1, a2 from test1 where a1 = 1 and a2 = 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for static hash:        "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test2 where a1 = 1 and a2 = 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for extensible hash:    "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test3 where a1 = 1 and a2 = 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for b-tree:             "+totalTime1);

            //test 3, select on one > operator
            System.out.println();
            start = System.nanoTime();
            query = "select a1, a2 from test1 having a1 > 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for static hash:        "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test2 having a1 > 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for extensible hash:    "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test3 having a1 > 1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for b-tree:             "+totalTime1);


            //now run JOIN tests
            System.out.println("\nRunning JOIN tests...\n");

            //test1 with table 4, join on 1 attribute
            start = System.nanoTime();
            query = "select a1, a2 from test1, test4 where a2 = test4.a1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for static hash:        "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test2, test4 where a2 = test4.a1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for extensible hash:    "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test3, test4 where a2 = test4.a1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for b-tree:             "+totalTime1);

            //test2 with table 5, join on 2 attributes
            System.out.println();
            start = System.nanoTime();
            query = "select a1, a2 from test1, test5 where a1 = test5.a2, a2 = test5.a2";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for static hash:        "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test2, test5 where a1 = test5.a2, a2 = test5.a2";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for extensible hash:    "+totalTime1);

            start = System.nanoTime();
            query = "select a1, a2 from test3, test5 where a1 = test5.a1, a2 = test5.a2";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for b-tree:             "+totalTime1);

            //test3 join with 2 other tables, join on 2 attributes
            System.out.println();
            start = System.nanoTime();
            query = "select a1 from test1, test4, test5 where a1 = test4.a1 and a1 = test5.a1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for static hash:        "+totalTime1);

            start = System.nanoTime();
            query = "select a1 from test2, test4, test5 where a1 = test4.a1 and a1 = test5.a1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for extensible hash:    "+totalTime1);

            start = System.nanoTime();
            query = "select a1 from test3, test4, test5 where a1 = test4.a1 and a1 = test5.a1";
            s.executeQuery(query);
            end = System.nanoTime();
            totalTime1 = end - start;
            System.out.println("Time for b-tree:             "+totalTime1);

            conn.close();

        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally
        {
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}

