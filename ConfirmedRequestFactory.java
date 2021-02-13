import java.sql.* ;
import org.apache.log4j.Logger ;

public class ConfirmedRequestFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(ConfirmedRequestFactory.class.getName());

    ConfirmedRequestFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    ConfirmedRequestFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new ConfirmedRequest(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
