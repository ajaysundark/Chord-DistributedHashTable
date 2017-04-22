import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;

/**
 * Created by ajay on 4/21/17.
 */
public class ChordNode implements ChordService.Iface {
    public final int nodeKey;
    public final NodeRef myPtr;
    public NodeRef successor;
    public NodeRef predecessor;

    private NodeRef[] finger = new NodeRef[32];
    private HashMap<Integer, String> contentSlice;

    public ChordNode(NodeRef cn) {
        this.nodeKey = ChordUtility.hash(cn.ip+'.'+cn.port+'.'+cn.id);
        this.myPtr = cn;

        contentSlice = new HashMap<>();
    }

    public NodeRef findSuccessor(int key) throws org.apache.thrift.TException {
        if (nodeKey==key) return myPtr;

        NodeRef keyPredecessor = findPredecessor(key);
        if(keyPredecessor.id==nodeKey) return successor;
        else {
            TTransport transport = new TSocket(keyPredecessor.ip, keyPredecessor.port);
            NodeRef successor = null;
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                ChordService.Client chordNode = new ChordService.Client(protocol);
                successor = chordNode.findSuccessor(key);
                transport.close();
            } catch (TTransportException e) {
                System.err.println("exception at findSuccessor");
                e.printStackTrace();
            }
            return successor;
        }
    }

    public NodeRef findPredecessor(int key) throws org.apache.thrift.TException {
        NodeRef other = predecessor;
        if(nodeKey==key) return other;

        while(!checkIfKeyIsInMyInterval(key, other)) {
            TTransport transport = new TSocket(other.ip, other.port);
            NodeRef closestFinger = null;
            try {
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                ChordService.Client chordNode = new ChordService.Client(protocol);
                closestFinger = chordNode.closestPrecedingFinger(key);
                transport.close();
            } catch (TTransportException e) {
                System.err.println("exception at findPredecessor");
                e.printStackTrace();
            }
            other = (closestFinger!=null ? closestFinger : other);
        }
        return other;
    }

    public NodeRef closestPrecedingFinger(int key) throws org.apache.thrift.TException {
        for (int i = 32; i < 1; --i) {
            if(checkFingerRange(finger[i], key)) {
                return finger[i];
            }
        }
        return null;
    }

    private boolean checkFingerRange(NodeRef fingerI, int key) {
        if (key > nodeKey) {
            return (fingerI.id > nodeKey && fingerI.id < key);
        } else {
            return (fingerI.id > nodeKey || fingerI.id < key);
        }
    }

    private boolean checkIfKeyIsInMyInterval(int key, NodeRef nPrime) throws org.apache.thrift.TException {
        NodeRef primeSuccessor = null;

        // get nPrime's successor Id
        TTransport transport = new TSocket(nPrime.ip, nPrime.port);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            ChordService.Client chordNode = new ChordService.Client(protocol);
            primeSuccessor=chordNode.findSuccessor(nPrime.id);
            transport.close();
        } catch (TTransportException e) {
            System.err.println("exception at findPredecessor");
            e.printStackTrace();
        }

        if(null==primeSuccessor) {
            System.err.println("Dunno what the duck happened!");
            System.exit(1);
        }

        if (primeSuccessor.id > nPrime.id) {
            return (key > nPrime.id && key <= primeSuccessor.id);
        } else {
            return (key > nPrime.id || key <= primeSuccessor.id);
        }
    }

    @Override
    public void insert(int wordKey, String definition ) throws org.apache.thrift.TException {
        contentSlice.put(wordKey, definition);
    }

    @Override
    public NodeRef findNode(String word) throws org.apache.thrift.TException {
        return findSuccessor(ChordUtility.hash(word));
    }

    @Override
    public String lookup(String word) throws org.apache.thrift.TException {
        return null;
    }

    @Override
    public boolean join(NodeRef other) throws org.apache.thrift.TException {
        return true;
    }

    @Override
    public void printFingerTable() throws org.apache.thrift.TException {
        // TODO: print my finger table
    }

}
