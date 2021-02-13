import java.sql.* ;
/*   $Author:   zf297a  $
   $Revision:   1.0  $
       $Date:   23 May 2007 00:19:32  $
   $Workfile:   DmndFrcstConsumablesFactory.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\DmndFrcstConsumablesFactory.java.-arc  $
/*
/*   Rev 1.0   23 May 2007 00:19:32   zf297a
/*Initial revision.
        */
public class DmndFrcstConsumablesFactory extends DBFactory
{
    DmndFrcstConsumablesFactory(String query) throws ClassNotFoundException, SQLException {
        super(query) ; 
    }
    public Rec createRec() {
        try {
            return new DmndFrcstConsumables(rs) ;
        }
        catch (SQLException e) {
            return null ;
        }
    }   
}
