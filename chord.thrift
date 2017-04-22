struct NodeRef {
    1: required string ip;
    2: required i32 port;
    3: required i32 id;
}

service ChordService {
    bool insert(1:string word, 2:string definition),
    NodeRef findNode(1:string word),
    string lookup(1:string word),
    bool join(1:NodeRef other),
    void printFingerTable(),
    NodeRef findSuccessor(1:i32 key),
    NodeRef findPredecessor(1:i32 key),
    NodeRef closestPrecedingFinger(1:i32 key)
}