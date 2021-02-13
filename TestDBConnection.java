// vim:ts=2:sw=2:sts=2:autoindent:smartindent:expandtab:
/*
 * TestDBConnection.java
 * Author: Douglas S. Elder
 * Date: 7/17/2017
 * Description: accept a connection string,
 * a user id, a password and connect to Oracle
 */

import java.sql.SQLException;

public class TestDBConnection {
  private static void usage() {
    System.out.println("java -cp ojdbc14.jar TestDBConnection.jar connectionString userid password") ;
  }
  public static void main(String args[]) {
    if (args.length != 3) {
      usage();
    }
    String connectionString = args[0]; 
    String uid = args[1];
    String pwd = args[2];
    try {
      DBConnection dbconn = new DBConnection(connectionString, uid, pwd) ;
    } catch (SQLException e) {
      System.out.println("Unable to connect: " + e.getMessage());
    } catch (ClassNotFoundException e) {
      System.out.println("ClassNotFoundException: " + e.getMessage());
    } catch (Exception e) {
      System.out.println("General Exception"  + e.getMessage());
    }
  }
}
