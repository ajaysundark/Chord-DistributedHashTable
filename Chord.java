import org.apache.thrift.TException;

/**
 * Created by ajay on 4/21/17.
 */
public class Chord {
    public final int chordNodesCount;
    private final String ip;
    private final int primaryPort;
    private final String dictSrc;
    // private ArrayList<chordProcessor> processors;

    public Chord(int count, String ip, int port, String srcFile) {
        this.chordNodesCount = count;
        this.ip = ip;
        this.primaryPort = port;
        this.dictSrc = srcFile;
        // this.processors = Arrays.asList(new ChordProcessor[chordNodesCount]);
    }

    public static void main(String args[]) {
        int noOfChordNodes = 8;
        new Chord(noOfChordNodes,
                args[0] /* ip */,
                Integer.valueOf(args[1]) /* port */,
                args[2] /* word-definition mappings */)
            .start();
    }

    public void start() {
        buildDHT();
        new DictionaryLoader(ip, primaryPort, dictSrc).load();
    }

    private void buildDHT() {
        /* create the node-0 chordNode */
        ChordNode primaryChord = new ChordNode(new NodeRef(ip, primaryPort, 0));
        startService(primaryChord);

        try {
            primaryChord.join(null);

        /* start remaining nodes and connect to Ring with node-0 join */
            for (int i = 1; i < chordNodesCount; i++) {
                ChordNode secondary = new ChordNode(new NodeRef(ip, primaryPort+i, i));
                startService(secondary);

                if(!secondary.join(primaryChord.myPtr)) {
                    System.out.println("JOIN FAILED: " + secondary.myPtr);
                }

            }
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    private void startService(ChordNode node) {
        // TODO: start thrift service for ChordNode
    }

    private void stopService() {
        // TODO: stop thrift service for all ChordNodes
    }
}
