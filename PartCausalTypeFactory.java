import java.sql.* ;
import org.apache.log4j.Logger ;

public class PartCausalTypeFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(PartCausalTypeFactory.class.getName());

    PartCausalTypeFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    PartCausalTypeFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new PartCausalType(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
