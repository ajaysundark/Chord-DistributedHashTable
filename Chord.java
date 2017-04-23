import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Created by ajay on 4/21/17.
 */
public class Chord {
    public final int chordNodesCount;
    private final String ip;
    private final int primaryPort;
    private final String dictSrc;
    private ArrayList<TServer> servers;

    public Chord(int count, String ip, int port, String srcFile) {
        this.chordNodesCount = count;
        this.ip = ip;
        this.primaryPort = port;
        this.dictSrc = srcFile;
        this.servers = new ArrayList<>(chordNodesCount);
    }

    public static void main(String args[]) {
        int noOfChordNodes = 3;
        if (args.length!=3) {
            System.err.println("Usage: java Chord ipaddr<str> port<int> dict-srcfile<str-path>");
            System.exit(1);
        }

        new Chord(noOfChordNodes,
                args[0] /* ip */,
                Integer.valueOf(args[1]) /* port */,
                args[2] /* word-definition mappings */)
            .start();
    }

    public void start() {
        buildDHT();
        System.out.println("-----------------------------------------");
        System.out.println(" DHT is ready.. You can load dictionary ");
        System.out.println("-----------------------------------------");
//        new DictionaryLoader(ip, primaryPort, dictSrc).load();
    }

    private void buildDHT() {
        /* create the node-0 chordNode */
        ChordNode primaryChord = new ChordNode(new NodeRef(ip, primaryPort, 0));
        startService(primaryChord);

        try {

            TimeUnit.SECONDS.sleep(4);
            primaryChord.join(null);

        /* start remaining nodes and connect to Ring with node-0 join */
            for (int i = 1; i < chordNodesCount; i++) {
                ChordNode secondary = new ChordNode(new NodeRef(ip, primaryPort+i, i));
                startService(secondary);

                if(!secondary.join(primaryChord.myPtr)) {
                    System.out.println("JOIN FAILED: " + secondary.myPtr);
                }

            }
        } catch (TException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void startService(ChordNode node) {
        ChordService.Processor processor = new ChordService.Processor(node);
        Runnable chordRunner = new Runnable() {
            public void run() {
                serve(processor, node.myPtr);
            }
        };
        new Thread(chordRunner).start();
    }

    protected static void serve(ChordService.Processor sProcessor, NodeRef connInfo) {
        try {
            TServerTransport serverTransport = new TServerSocket(connInfo.port);
            TServer server = new TSimpleServer(new TServer.Args(serverTransport).processor(sProcessor));
            System.out.println("Starting the Chord server "+connInfo.nodeId+" at port "+connInfo.port);
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    private void stopService() {
        for (TServer server : servers) {
            server.stop();
        }
    }
}
