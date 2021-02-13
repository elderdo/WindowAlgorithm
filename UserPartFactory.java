import java.sql.* ;
import org.apache.log4j.Logger ;

public class UserPartFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(UserPartFactory.class.getName());

    UserPartFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    UserPartFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new UserPart(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
