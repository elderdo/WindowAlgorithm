import java.sql.* ;
import org.apache.log4j.Logger ;

public class ConfirmedRequestLineFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(ConfirmedRequestLineFactory.class.getName());

    ConfirmedRequestLineFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    ConfirmedRequestLineFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new ConfirmedRequestLine(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
