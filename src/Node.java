import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Node extends UnicastRemoteObject implements NodeInterface {
  private String name;
  private String hashedId;
  private boolean recoverStatus;
  private NodeInterface successor;
  private NodeInterface predecessor;
  private HashMap<Integer, NodeInterface> membershipTable;
  private HashMap<String, String> storage;

  private HashMap<String, Timer> heartBeaterTimerMap;
  private HashMap<String, HeartBeater> heartBeaterTaskMap;

  private static final String NAME_PREFIX = "vm-";
  public static final int HASH_BIT = 7;

  Node(String vmId) throws RemoteException {
    this.name = NAME_PREFIX + vmId;
    this.hashedId = ConsistentHashing.generateHashedId(this.name, (int)Math.pow(2, HASH_BIT));
    this.recoverStatus = false;
    this.successor = this;
    this.predecessor = this;
    this.membershipTable = new HashMap<>();
    this.storage = new HashMap<>();

    this.heartBeaterTimerMap = new HashMap<>();
    this.heartBeaterTaskMap = new HashMap<>();

    Registry registry;
    int port = Integer.parseInt("100" + vmId);
    registry = LocateRegistry.createRegistry(port);
    registry.rebind(this.name, this);
  }

  /**
   * Set up Chord structure. Decide predecessor and successor of the node.
   * @param addressList
   */
  public void setupChord(ArrayList<String> addressList) {
    // Connect to all the active remote nodes
    ArrayList<NodeInterface> nodeInterfaceList = new ArrayList<>();
    for (int i = 0; i < addressList.size(); i++) {
      String remoteIp = addressList.get(i).split(" ")[0];
      String remoteId = addressList.get(i).split(" ")[1];

      int remotePort = Integer.parseInt("100" + remoteId);
      String remoteName = NAME_PREFIX + remoteId;

      if (remoteName.equals(this.name))
        continue;
      try {
        Registry registry = LocateRegistry.getRegistry(remoteIp, remotePort);
        NodeInterface remoteNode = (NodeInterface) registry.lookup(remoteName);

        nodeInterfaceList.add(remoteNode);
      } catch (Exception e) {
        // Exception indicates the node is not online, just skip
        continue;
      }
    }

    boolean joinDelay;
    do {
      joinDelay = false;

      try {
        Thread.sleep(1000);
      } catch (Exception joinDelayE) {
        System.err.println("[Join Delay Exception]" + joinDelayE);
      }

      for (NodeInterface remoteNode : nodeInterfaceList) {
        try {
          if (remoteNode.getRecoverStatus()) {
            joinDelay = true;
            break;
          }
        } catch (Exception joinTraversalE) {
          System.err.println("[Join Traversal Exception]" + joinTraversalE);
        }
      }
    } while (joinDelay);

    Collections.sort(nodeInterfaceList, new NodeInterfaceComparator());
    NodeInterface tempPred = nodeInterfaceList.get(nodeInterfaceList.size() - 1);
    NodeInterface tempSucc = nodeInterfaceList.get(0);
    try {
      // Find predecessor and successor
      for (int i = 0; i < nodeInterfaceList.size(); i++) {
        if (Integer.parseInt(nodeInterfaceList.get(i).getHashedId()) < Integer.parseInt(this.hashedId)) {
          continue;
        }
        if (i != 0) {
          tempPred = nodeInterfaceList.get(i - 1);
          tempSucc = nodeInterfaceList.get(i);
        }
        break;
      }

      // link the ring
      tempPred.setSuccessor(this);
      tempSucc.setPredecessor(this);
      setSuccessor(tempSucc);
      setPredecessor(tempPred);

      // Build membership table
      ArrayList<NodeInterface> nodeList = getAllNodes();
      buildMembershipTable(nodeList);
      for (NodeInterface node : nodeList) {
        if (!node.getHashedId().equals(hashedId)) {
          node.updateMembershipTable();
        }
      }

      // Setup HeartBeater
      for (Integer hashedIdValue : membershipTable.keySet()) {
        if (!String.valueOf(hashedIdValue).equals(hashedId)) {
          setupHeartBeat(String.valueOf(hashedIdValue));
          membershipTable.get(hashedIdValue).setupHeartBeat(hashedId);
        }
      }

      ArrayList<Integer> membershipList = new ArrayList<>(membershipTable.keySet());
      NodeInterface leader = membershipTable.get(Collections.max(membershipList));

      // Rebalance keys
//      rebalance(this);
      leader.rebalance();
    } catch (Exception setupChordE) {
      System.err.println("[SetupChord Exception]" + setupChordE);
    }
  }

  /**
   * Current node leave the network, handover its keys and other nodes rebuild the ring structure
   */
  public void leave() {
    try {
      // Hand over storage to successors
      for (String key : storage.keySet()) {
        // Always keep 3 replicas
        if (successor.getSuccessor().getLocal(key) != null && successor.getLocal(key) != null) {
          predecessor.putLocal(key, storage.get(key));
        } else if (successor.getSuccessor().getLocal(key) == null && successor.getLocal(key) != null) {
          successor.getSuccessor().putLocal(key, storage.get(key));
        } else {
          successor.putLocal(key, storage.get(key));
        }
      }

      ArrayList<NodeInterface> nodeList = getAllNodes();
      nodeList.remove(nodeList.indexOf(this));

      // Exit the ring
      successor.setPredecessor(predecessor);
      predecessor.setSuccessor(successor);
      predecessor = this;
      successor = this;

      // Update all nodes' membership table, and remove heartbeat
      for (NodeInterface node : nodeList) {
        if (!node.getHashedId().equals(hashedId)) {
          node.buildMembershipTable(nodeList);
          node.removeHeartBeat(hashedId);
        }
      }
    } catch (Exception leaveE) {
      System.err.println("[Leave Exception]" + leaveE);
    }
  }

  /**
   * Get all the nodes in the network.
   * @return An ArrayList of nodes
   */
  public ArrayList<NodeInterface> getAllNodes() {
    ArrayList<NodeInterface> nodeList = new ArrayList<>();

    try {
      NodeInterface curNode = this;
      do {
        nodeList.add(curNode);
        curNode = curNode.getSuccessor();
      } while (!curNode.getName().equals(this.getName()));
    } catch (RemoteException e) {
      System.err.println("Exception4: " + e);
    }

    return nodeList;
  }

  /**
   * Put a key-value pair into the distributed store
   * @param key
   * @param value
   */
  public void put(String key, String value) {
    String hashedId = ConsistentHashing.generateHashedId(key, (int)Math.pow(2, HASH_BIT));

    try {
      NodeInterface targetNode = findNodeByHashedId(hashedId);

      boolean putDelay;
      do {
        putDelay = false;

        try {
          Thread.sleep(1000);
        } catch (Exception putDelayE) {
          System.err.println("[Put Delay Exception]" + putDelayE);
        }

        for (Integer remoteHasheIdValue : membershipTable.keySet()) {
          try {
            if (membershipTable.get(remoteHasheIdValue).getRecoverStatus()) {
              putDelay = true;
              break;
            }
          } catch (Exception putTraversalE) {
            System.err.println("[Put Traversal Exception]" + putTraversalE);
          }
        }
      } while (putDelay);

      targetNode.putLocal(key, value);
      // Back up replicas in predecessor and successor
      targetNode.getPredecessor().putLocal(key, value);
      targetNode.getSuccessor().putLocal(key, value);
    } catch (Exception putE) {
      System.err.println("[Set Key Exception]" + putE);
    }
  }

  /**
   * Get a value with key from the distributed store
   * @param key
   * @return value associated with the key
   */
  public String get(String key) {
    String hashedId = ConsistentHashing.generateHashedId(key, (int)Math.pow(2, HASH_BIT));
    String value = null;

    try {
      NodeInterface targetNode = findNodeByHashedId(hashedId);
      value  = targetNode.getLocal(key);
    } catch (RemoteException getE) {
      System.err.println("[Get Key Exception]" + getE);
    }

    return value;
  }

  /**
   * Find nodes who store the key
   * @param key
   * @return nodes in ArrayList
   */
  public ArrayList<NodeInterface> findOwners(String key) {
    String hashedId = ConsistentHashing.generateHashedId(key, (int)Math.pow(2, HASH_BIT));
    ArrayList<NodeInterface> owners = new ArrayList<>();

    try {
      NodeInterface targetNode = findNodeByHashedId(hashedId);
      if (targetNode.getLocal(key) != null) {
        owners.add(targetNode);
      }
      // Check replicas
      if (targetNode.getPredecessor().getLocal(key) != null) {
        owners.add(targetNode.getPredecessor());
      }
      if (targetNode.getSuccessor().getLocal(key) != null) {
        owners.add(targetNode.getSuccessor());
      }
    } catch (RemoteException findOwnersE) {
      System.err.println("[Find Owners Exception]" + findOwnersE);
    }

    return owners;
  }

  /**
   * Get heartBeaterTaskMap. Mainly for debugging
   * @return heartbeater task map in HashMap
   */
  public HashMap<String, HeartBeater> getHeartBeaterTaskMap() {
    return heartBeaterTaskMap;
  }

  /* NodeInterface Implementation */

  @Override
  public void buildMembershipTable(ArrayList<NodeInterface> nodeList) throws RemoteException {
    Collections.sort(nodeList, new NodeInterfaceComparator());
    HashMap<Integer, NodeInterface> membershipTable = new HashMap<>();

    try {
      for (NodeInterface curNode : nodeList ) {
        int curHashedId = Integer.parseInt(curNode.getHashedId());
        membershipTable.put(curHashedId, curNode);
      }
    } catch (RemoteException e) {
      System.err.println("Exception8: " + e);
    }

    this.membershipTable = membershipTable;
  }

  @Override
  public void updateMembershipTable() throws RemoteException {
    ArrayList<NodeInterface> nodeList = getAllNodes();
    for (NodeInterface node : nodeList) {
      if (membershipTable.containsKey(Integer.parseInt(node.getHashedId()))) {
        continue;
      }
      membershipTable.put(Integer.parseInt(node.getHashedId()), node);
    }
  }

  @Override
  public void rebalance() throws RemoteException {
    // Get all keys in the system
    HashMap<String, String> allKeysMap = new HashMap<>();
    try {
      for (Integer hashedIdValue : membershipTable.keySet()) {
        HashMap<String, String> localStorage = membershipTable.get(hashedIdValue).getLocalStorage();
        for (String key : localStorage.keySet()) {
          allKeysMap.put(key, localStorage.get(key));
        }
      }
    } catch (Exception getAllKeyE) {
      System.err.println("[Get All Keys Exception]" + getAllKeyE);
    }

    // Remove all keys in the system
    try {
      for (Integer hashedIdValue : membershipTable.keySet()) {
        HashMap<String, String> localStorageReplica = new HashMap<>(membershipTable.get(hashedIdValue).getLocalStorage());
        for (String key : localStorageReplica.keySet()) {
          membershipTable.get(hashedIdValue).removeLocal(key);
        }
      }
    } catch (Exception getAllKeyE) {
      System.err.println("[Remove All Keys Exception]" + getAllKeyE);
    }

    // Distribute all keys
    try {
      for (String key : allKeysMap.keySet()) {
        String value = allKeysMap.get(key);
        String keyHashedId = ConsistentHashing.generateHashedId(key, (int)Math.pow(2, Node.HASH_BIT));
        NodeInterface keyHashedNode = findNodeByHashedId(keyHashedId);

        keyHashedNode.putLocal(key, value);
        keyHashedNode.getPredecessor().putLocal(key, value);
        keyHashedNode.getSuccessor().putLocal(key, value);
      }
    } catch (Exception distributeKeyE) {
      System.err.println("[Distribute Keys Exception]" + distributeKeyE);
    }
  }

  @Override
  public String getName() throws RemoteException {
    return this.name;
  }

  @Override
  public String getHashedId() throws RemoteException {
    return this.hashedId;
  }

  @Override
  public boolean getRecoverStatus() throws RemoteException {
    return this.recoverStatus;
  }

  @Override
  public void setRecoverStatus(boolean flag) throws RemoteException {
    this.recoverStatus = flag;
  }

  @Override
  public HashMap<Integer, NodeInterface> getMembershipTable() throws RemoteException {
    return this.membershipTable;
  }

  @Override
  public void removeMembership(Integer hashedIdValue) throws RemoteException {
    membershipTable.remove(hashedIdValue);
  }

  @Override
  public HashMap<String, String> getLocalStorage() throws RemoteException {
    return this.storage;
  }

  @Override
  public NodeInterface getSuccessor() throws RemoteException {
    return this.successor;
  }

  @Override
  public NodeInterface getPredecessor() throws RemoteException {
    return this.predecessor;
  }

  @Override
  public void setSuccessor(NodeInterface succ) throws RemoteException {
    this.successor = succ;
  }

  @Override
  public void setPredecessor(NodeInterface pred) throws RemoteException {
    this.predecessor = pred;
  }

  @Override
  public NodeInterface findNodeByHashedId(String hashedId) throws RemoteException {
    ArrayList<Integer> hashedIdList = new ArrayList<>(membershipTable.keySet());
    Collections.sort(hashedIdList);
    int hashedIdValue = Integer.parseInt(hashedId);
    NodeInterface targetNode = this;

    if (hashedIdValue <= hashedIdList.get(0) || hashedIdValue > hashedIdList.get(hashedIdList.size() - 1)) {
      targetNode = membershipTable.get(hashedIdList.get(0));
    } else {
      for (int i = 1; i < hashedIdList.size(); i++) {
        if (hashedIdList.get(i) >= hashedIdValue) {
          targetNode = membershipTable.get(hashedIdList.get(i));
          break;
        }
      }
    }

    return targetNode;
  }

  @Override
  public void putLocal(String key, String value) throws RemoteException {
    storage.put(key, value);
  }

  @Override
  public String getLocal(String key) throws RemoteException {
    return storage.get(key);
  }

  @Override
  public void removeLocal(String key) throws RemoteException {
    storage.remove(key);
  }

  @Override
  public void setupHeartBeat(String hashedId) throws RemoteException {
    if (!heartBeaterTaskMap.containsKey(hashedId)) {
      NodeInterface remoteNode = membershipTable.get(Integer.parseInt(hashedId));
      try {
        heartBeaterTimerMap.put(hashedId, new Timer(true));
        heartBeaterTaskMap.put(hashedId, new HeartBeater(this, remoteNode));
        heartBeaterTimerMap.get(hashedId).schedule(heartBeaterTaskMap.get(hashedId), 0, 500);
      } catch (Exception setupHeartBeatE) {
        System.err.println("[Setup HeartBeat Exception]" + setupHeartBeatE);
      }
    }
  }

  @Override
  public void removeHeartBeat(String hashedId) throws RemoteException {
    heartBeaterTimerMap.get(hashedId).cancel();
    heartBeaterTaskMap.get(hashedId).cancel();
    heartBeaterTimerMap.remove(hashedId);
    heartBeaterTaskMap.remove(hashedId);
  }

  public class NodeInterfaceComparator implements Comparator<NodeInterface> {
    @Override
    public int compare(NodeInterface a, NodeInterface b) {
      try {
        return Integer.parseInt(a.getHashedId()) - Integer.parseInt(b.getHashedId());
      } catch (RemoteException comparisionE) {
        System.err.println("[Comparision Exception]" + comparisionE);
        return -1;
      }
    }
  }
}


