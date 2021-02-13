import java.sql.* ;
import org.apache.log4j.Logger ;

public class LpOverrideFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(LPOverrideConsumablesFactory.class.getName());

    LpOverrideFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    LpOverrideFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new LpOverride(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
