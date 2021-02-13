import java.sql.* ;
import org.apache.log4j.Logger ;

public class SpoPartFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(SpoPartFactory.class.getName());

    SpoPartFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    SpoPartFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new SpoPart(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
