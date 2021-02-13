import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpDemandFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LpDemandFactory.class.getName());

    LpDemandFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpDemandFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpDemand(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
