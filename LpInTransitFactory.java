import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpInTransitFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LpInTransitFactory.class.getName());

    LpInTransitFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpInTransitFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpInTransit(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
