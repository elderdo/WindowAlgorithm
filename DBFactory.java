import java.sql.* ;
import oracle.jdbc.driver.OracleStatement;
import org.apache.log4j.Logger ;

public abstract class DBFactory implements RecFactory
{



  	static Logger logger = Logger.getLogger(DBFactory.class.getName());
  	String query ;
    protected ResultSet rs ;

    private boolean isValidQuery() {
        return true ;
    }
    
    DBFactory(String query) throws SQLException {
	      this.query = query ;
        AmdConnection amd = AmdConnection.instance() ;

        PreparedStatement s = amd.c.prepareStatement(query) ;

        ((OracleStatement)s).setRowPrefetch(100) ;
        rs = s.executeQuery(query);
    }

    DBFactory(String query, DBConnection connection) throws SQLException {

	      this.query = query ;
        PreparedStatement s = connection.c.prepareStatement(query); 
        rs = s.executeQuery();
	      logger.debug("Factory created for " + query.replace('\n','_').replace('\r','_')) ;

    }

    DBFactory(String query, DBConnection connection, int prefetchValue) throws SQLException {

      	this.query = query ;
        PreparedStatement s = connection.c.prepareStatement(query); 

        ((OracleStatement)s).setFetchSize(prefetchValue) ;

        rs = s.executeQuery();
	      logger.debug("Factory created for " + query.replace('\n','_').replace('\r','_')) ;

    }

    public boolean moreRecsToMake() {
        try {
            return rs.next() ;
        }
        catch (SQLException e) {
            return false ;
        }
    }

    public String getQuery() {
	    logger.debug("getQuery") ;
	    return query ;
    } ;

}
