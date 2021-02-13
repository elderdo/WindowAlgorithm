/*   $Author:   c970183  $
   $Revision:   1.0  $
       $Date:   30 Aug 2004 08:11:12  $
   $Workfile:   PartLocsFactory.java  $
        $Log:   \\www-amssc-01\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\PartLocsFactory.java-arc  $
/*
/*   Rev 1.0   30 Aug 2004 08:11:12   c970183
/*Initial revision.
*/
import java.sql.* ;
import org.apache.log4j.Logger ;

public class PartLocsFactory extends DBFactory
{
    static Logger logger = Logger.getLogger(WindowAlgo.class.getName());

    PartLocsFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ;
    }
    public Rec createRec() {
        try {
            return new PartLocs(rs) ;
        }
        catch (SQLException e) {
			logger.debug("SQLException " + e.getMessage()) ;
            return null ;
        }
    }
}