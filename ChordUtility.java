/**
 * Created by ajay on 4/21/17.
 */
public class ChordUtility {
    public static int hash(Object object) {
        if(object.hashCode()<0)
            return object.hashCode()>>>1;
        else return object.hashCode();
    }

    public static int hash(NodeRef node) {
        // TODO: handle dummy hash
        return (node.port%1000)*2;
    }

}
