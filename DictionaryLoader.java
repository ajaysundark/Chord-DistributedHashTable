import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Created by ajay on 4/21/17.
 */
public class DictionaryLoader {
    private final String dictSource;
    private final String ip;
    private final int port;

    public DictionaryLoader(String ip, int primaryPort, String srcFile) {
        this.ip = ip;
        this.port = primaryPort;
        this.dictSource = srcFile;
    }

    public void load() {
        try(Stream<String> contentStream = Files.lines(Paths.get(dictSource))) {
            contentStream.forEach(this::insertToChord);
        } catch (IOException ioe) { ioe.printStackTrace();}
    }

    private void insertToChord(String line) {
        String parts[] = line.split(":");
        int hash = ChordUtility.hash(parts[0].trim());

        NodeRef successor = null;
        TTransport transport = new TSocket(ip, port);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            ChordService.Client chordNode = new ChordService.Client(protocol);
            successor = chordNode.findSuccessor(hash);
            transport.close();
        } catch (TException e) {
            System.err.println("exception at findPredecessor");
            e.printStackTrace();
        }

        if(successor!=null) {
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                ChordService.Client chordNode = new ChordService.Client(protocol);
                chordNode.insert(hash, parts[1].trim());
                transport.close();
            } catch (TException e) {
                System.err.println("exception at findPredecessor");
                e.printStackTrace();
            }
        }

    }
}
