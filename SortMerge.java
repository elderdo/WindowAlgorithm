public class SortMerge
{
   public void diff(SortedSnapshot F1, SortedSnapshot F2) {
        Rec r1 = F1.read() ;
        Rec r2 = F2.read() ;
        while (r1 != null && r2 != null) {
            if (r1 == null || r1.getKey().compareTo(r2.getKey()) > 0) {
                // r1.Key > r2.Key
                r2.insert() ;
                r2 = F2.read() ;
            }
            else if (r2 == null || r1.getKey().compareTo(r2.getKey()) < 0) {
                // r1.Key < r2.Key
                r1.delete() ;
                r1 = F1.read() ;
            }
            else {
                // r1.Key == r2.Key
                if (!r1.getBody().equals(r2.getBody()))
                    r2.update() ;
                r1 = F1.read() ;
                r2 = F2.read() ;                
            }
        }        
   }
}