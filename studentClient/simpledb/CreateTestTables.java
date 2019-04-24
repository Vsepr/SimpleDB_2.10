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

            s.executeUpdate("create sh index idx1 on test1 (a1)");
            s.executeUpdate("create ex index idx2 on test2 (a1)");
            s.executeUpdate("create bt index idx3 on test3 (a1)");
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

            long startTime1 = System.nanoTime();
            String query = "select a1,a2 from test1 where a1 = 250";
            s.executeUpdate(query);
            long endTime1   = System.nanoTime();
            long totalTime1 = endTime1 - startTime1;
            System.out.println("Running time for test1 is:"+totalTime1);

/*
            long startTime1 = System.nanoTime();
            s.executeUpdate("SELECT a1,a2 from test5, test1 where test5.a1 = test1.a1");
            long endTime1   = System.nanoTime();
            long totalTime1 = endTime1 - startTime1;
            System.out.println("Running time for test1 is:"+totalTime1);
            long startTime2 = System.nanoTime();
            s.executeUpdate("SELECT a1,a2 from test5, test2 where test5.a1 = test2.a1");
            long endTime2   = System.nanoTime();
            long totalTime2 = endTime2 - startTime2;
            System.out.println("Running time for test2 is:"+totalTime2);
            long startTime3 = System.nanoTime();
            s.executeUpdate("SELECT a1,a2 from test5, test3 where test5.a1 = test3.a1");
            long endTime3   = System.nanoTime();
            long totalTime3 = endTime3 - startTime3;
            System.out.println("Running time for test3 is:"+totalTime3);
            long startTime4 = System.nanoTime();
            s.executeUpdate("SELECT a1,a2 from test5, test4 where test5.a1 = test4.a1");
            long endTime4   = System.nanoTime();
            long totalTime4 = endTime4 - startTime4;
            System.out.println("Running time for test4 is:"+totalTime4);
*/

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

