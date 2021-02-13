import java.sql.* ;
import org.apache.log4j.Logger ;

public class PartPlannedPartFactory extends DBFactory
{
   static Logger logger = Logger.getLogger(PartPlannedPartFactory.class.getName());

    PartPlannedPartFactory(String query, DBConnection connection, int prefetchValue) throws ClassNotFoundException, SQLException {
        super(query, connection, prefetchValue) ; 
	logger.debug("query=" + query) ;
	logger.debug("prefetch=" + prefetchValue) ;
    }

    PartPlannedPartFactory(String query) throws ClassNotFoundException, SQLException {
	super(query);
	logger.debug("query=" + query) ;
    }
    public Rec createRec() {
        try {
            return new PartPlannedPart(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
