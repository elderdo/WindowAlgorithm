import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpOnHandFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LpOnHandFactory.class.getName());

    LpOnHandFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpOnHandFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpOnHand(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
