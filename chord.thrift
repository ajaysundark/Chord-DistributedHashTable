struct NodeRef {
    1: required string ip;
    2: required i32 port;
    3: required i32 nodeId;
}

struct Finger {
    1: required i32 start;
    2: required NodeRef node;
}

service ChordService {
    void insert(1:i32 wordKey, 2:string definition),
    NodeRef findNode(1:string word),
    string lookup(1:string word),
    bool join(1:NodeRef other),
    void printFingerTable(),
    NodeRef findSuccessor(1:i32 key),
    NodeRef findPredecessor(1:i32 key),
    NodeRef closestPrecedingFinger(1:i32 key),
    void updatePredecessor(1:NodeRef other),
    void updateFingerTables(1:NodeRef s, 2:i32 i)
}