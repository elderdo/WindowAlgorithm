import java.sql.* ;
import org.apache.log4j.Logger ;

public class NetworkPartFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(NetworkPartFactory.class.getName());

    NetworkPartFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    NetworkPartFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new NetworkPart(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
