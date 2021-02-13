import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpLeadTimeFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LpLeadTimeFactory.class.getName());

    LpLeadTimeFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpLeadTimeFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpLeadTime(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
