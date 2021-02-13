import java.sql.* ;
import org.apache.log4j.Logger ;

public class SpoUserFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(SpoUserFactory.class.getName());

    SpoUserFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    SpoUserFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new SpoUser(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
