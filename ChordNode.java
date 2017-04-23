import org.apache.thrift.TException;
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

    private Finger[] finger = new Finger[32];
    private HashMap<Integer, String> contentSlice;

    public ChordNode(NodeRef cn) {
        this.nodeKey = ChordUtility.hash(cn.ip+'.'+cn.port+'.'+cn.nodeId);
        this.myPtr = new NodeRef(cn);

        this.successor = this.myPtr;
        this.predecessor = this.myPtr;
        contentSlice = new HashMap<>();
    }

    @Override
    public void updatePredecessor(NodeRef other) throws org.apache.thrift.TException {
        this.predecessor = other;
    }

    @Override
    public NodeRef findSuccessor(int key) throws org.apache.thrift.TException {
        if (nodeKey==key) return myPtr;

        NodeRef keyPredecessor = findPredecessor(key);
        if(keyPredecessor.nodeId==nodeKey) return successor;
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
            return successor==null? null : new NodeRef(successor);
        }
    }

    @Override
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

            if(closestFinger==null) break;
            else other = closestFinger;
        }
        return other;
    }

    @Override
    public NodeRef closestPrecedingFinger(int key) throws org.apache.thrift.TException {
        for (int i = 31; i >= 0; --i) {
            if(checkFingerRange(finger[i].node, key)) {
                return finger[i].node;
            }
        }
        return null;
    }

    private boolean checkFingerRange(NodeRef fingerI, int key) {
        if (key > nodeKey) {
            return (fingerI.nodeId > nodeKey && fingerI.nodeId < key);
        } else {
            return (fingerI.nodeId > nodeKey || fingerI.nodeId < key);
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
            primeSuccessor=chordNode.findSuccessor(nPrime.nodeId);
            transport.close();
        } catch (TTransportException e) {
            System.err.println("exception at findPredecessor");
            e.printStackTrace();
        }

        if(null==primeSuccessor) {
            System.err.println("Dunno what the duck happened!");
            System.exit(1);
        }

        if (primeSuccessor.nodeId > nPrime.nodeId) {
            return (key > nPrime.nodeId && key <= primeSuccessor.nodeId);
        } else {
            return (key > nPrime.nodeId || key <= primeSuccessor.nodeId);
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
        return contentSlice.get(ChordUtility.hash(word));
    }

    @Override
    public boolean join(NodeRef other) throws org.apache.thrift.TException {
        if(other!=null) {
            initFingerTable(other);
            updateOthers();
        } else {
            // self join
            for (int i = 0; i < 32; i++) {
                this.finger[i] = new Finger((nodeKey + (1<<i))%(1<<32), new NodeRef(myPtr));
            }
            this.predecessor = new NodeRef(myPtr);
            this.successor = this.finger[0].node;
        }
        return true;
    }

    private void updateOthers() {
        for (int i = 0; i < 32; i++) {
            int pkey = nodeKey-(1<<i);
            if(pkey < 0) pkey+=(1<<32);

            try {
                NodeRef pred = findPredecessor(pkey);
                // update others finger table
                TTransport transport = new TSocket(pred.ip, pred.port);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                ChordService.Client chordNode = new ChordService.Client(protocol);
                chordNode.updateFingerTables(myPtr, i);
                transport.close();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void updateFingerTables(NodeRef s, int i) throws org.apache.thrift.TException {
        if ( (s.nodeId >= nodeKey && s.nodeId<finger[i].node.nodeId)
                || (s.nodeId<finger[i].node.nodeId && s.nodeId >= nodeKey) ){
            finger[i].node = new NodeRef(s);
            if(i==1) this.successor = finger[i].node;

            try {
                // update others finger table
                TTransport transport = new TSocket(predecessor.ip, predecessor.port);
                transport.open();
                TProtocol protocol = new TBinaryProtocol(transport);
                ChordService.Client chordNode = new ChordService.Client(protocol);
                chordNode.updateFingerTables(s, i);
                transport.close();
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }

    private void initFingerTable(NodeRef other) {
        // update first finger, which is also the successor
        TTransport transport = new TSocket(other.ip, other.port);
        try {
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            ChordService.Client chordNode = new ChordService.Client(protocol);
            this.successor = chordNode.findSuccessor((nodeKey+(1<<0))%(1<<32) ); // first finger : n + pow(2, i) mod pow(2,m)
            transport.close();
        } catch (TException e) {
            System.err.println("exception at initFingerTable");
            e.printStackTrace();
        }
        this.finger[0] = new Finger((nodeKey+(1<<0))%(1<<32), successor);

        // update predecessor
        TTransport transport2 = new TSocket(successor.ip, successor.port);
        try {
            transport2.open();
            TProtocol protocol = new TBinaryProtocol(transport2);
            ChordService.Client chordNode = new ChordService.Client(protocol);
            this.predecessor = chordNode.findPredecessor(successor.nodeId);
        // update successor's predecessor
            chordNode.updatePredecessor(new NodeRef(myPtr));
            transport.close();
        } catch (TException e) {
            System.err.println("exception at initFingerTable");
            e.printStackTrace();
        }

        for (int i = 0; i < 31; i++) {
            /* finger[i+1].start */
            int fstart = ( (nodeKey + (1<<(i+1)))%(1<<32) ); // start = n+pow(2,i+1) mod pow(2, 32)
            if ( (fstart >= nodeKey && fstart<finger[i].node.nodeId)
                    || (fstart<finger[i].node.nodeId && fstart >= nodeKey) ){
                this.finger[i+1] = new Finger(fstart, new NodeRef(finger[i].node));
            } else {
                // update with other's successor, which is also the successor
                TTransport transport3 = new TSocket(other.ip, other.port);
                try {
                    transport3.open();
                    TProtocol protocol = new TBinaryProtocol(transport3);
                    ChordService.Client chordNode = new ChordService.Client(protocol);
                    NodeRef otherSuccessor = chordNode.findSuccessor(fstart); // first finger : n + pow(2, i) mod pow(2,m)
                    this.finger[i+1] = new Finger(fstart, otherSuccessor);
                    transport.close();
                } catch (TException e) {
                    System.err.println("exception at initFingerTable");
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public void printFingerTable() throws org.apache.thrift.TException {
        // TODO: print my finger table
    }

}
