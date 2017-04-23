import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.Console;

/**
 * Created by ajay on 4/23/17.
 */
public class ChordClient {
    private enum ClientActions { PRINT_TABLE, FIND, LOOKUP };

    public static void main(String args[]) {
        if (args.length!=1) {
            System.err.println("Usage: java ChordClient node-0-url <ip:port>");
            System.exit(1);
        }

        String parts[] = args[0].split(":");
        final String host = parts[0].trim();
        final String port = parts[0].trim();
        
        treatConsole(host,port);
    }

    private static void treatConsole(String host, String port) {
        Console console = System.console();
        while(true) {
            System.out.println("client> ");
            String line = console.readLine();
            String[] parts = line.split(":");
            String cmd = parts[0];

            switch (cmd) {
                case "ftable":
                    query(host, port, null, ClientActions.PRINT_TABLE);
                case "find":
                    String result = query(host, port, parts[1], ClientActions.FIND);
                    System.out.println(result);
                case "quit":
                    break;
                default:
                    System.out.println("Dictionary Usage: " +
                            "ftable -- print finger table\n " +
                            "find -- find a word, takes second argument word\n");
            }
        }
    }

    private static String query(String host, String port, String arg, ClientActions action) {
        String result = null;
        try {
            TTransport serverPipe = new TSocket(host, Integer.parseInt(port));
            serverPipe.open();
            TProtocol protocol = new TBinaryProtocol(serverPipe);
            ChordService.Client node0 = new ChordService.Client(protocol);
            result = perform(node0, arg, action);
            serverPipe.close();
        } catch (TException te) { te.printStackTrace(); }
        return (result==null ? "" : result);
    }

    private static String perform(ChordService.Client node0, String arg, ClientActions action) throws TException {
        switch (action) {
            case FIND:
                NodeRef n = node0.findNode(arg);
                return query(n.ip, String.valueOf(n.port), arg, ClientActions.LOOKUP);
            case LOOKUP:
                return node0.lookup(arg);
            case PRINT_TABLE:
                node0.printFingerTable();
                return null;
        }
        return null;
    }
}
