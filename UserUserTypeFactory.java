import java.sql.* ;
import org.apache.log4j.Logger ;

public class UserUserTypeFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(UserUserTypeFactory.class.getName());

    UserUserTypeFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    UserUserTypeFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new UserUserType(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
