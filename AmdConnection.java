import java.sql.* ;
import java.io.* ;
import java.util.Properties ;
/*   $Author:   zf297a  $
   $Revision:   1.5  $
       $Date:   14 Dec 2007 20:19:14  $
   $Workfile:   AmdConnection.java  $
        $Log:   I:\Program Files\Merant\vm\win32\bin\pds\archives\SDS-AMD\Components-ClientServer\WindowAlgorithm\AmdConnection.java-arc  $
   
      Rev 1.5   14 Dec 2007 20:19:14   zf297a
   Made this class a subclass of DBConnection
   
      Rev 1.4   17 Sep 2002 07:47:38   c970183
   Accepts an ini file containing the connection string, userid, and password via a setIniFile method
   
      Rev 1.3   26 Aug 2002 11:08:44   c970183
   Added PVCS keywords
   
  */

public class AmdConnection extends DBConnection
{
        static private AmdConnection instance_ = new AmdConnection() ; 

        static public AmdConnection instance() {
                return instance_ ;
        }

}
