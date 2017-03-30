import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class Node extends UnicastRemoteObject implements NodeInterface {
  private String name;
  private String hashedId;
  private NodeInterface successor;
  private NodeInterface predecessor;
  private ArrayList<NodeInterface> fingerTable;
  private HashMap<String, String> storage;

  private static final int HASH_BIT = 7;
  private static final String NAME_PREFIX = "vm-";

  Node(String vmId) throws RemoteException {
    this.name = NAME_PREFIX + vmId;
    this.hashedId = ConsistentHashing.generateHashedId(this.name, (int)Math.pow(2, HASH_BIT));
    this.successor = this;
    this.predecessor = this;
    this.fingerTable = new ArrayList<>();
    this.storage = new HashMap<>();

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
    try {
      ArrayList<NodeInterface> nodeInterfaceList = new ArrayList<>();
      for (int i = 0; i < addressList.size(); i++) {
        String remoteIp = addressList.get(i).split(" ")[0];
        String remoteId = addressList.get(i).split(" ")[1];

        int remotePort = Integer.parseInt("100" + remoteId);
        String remoteName = NAME_PREFIX + remoteId;

        if (remoteName.equals(this.name))
          continue;

        Registry registry = LocateRegistry.getRegistry(remoteIp, remotePort);
        NodeInterface remoteNode = (NodeInterface) registry.lookup(remoteName);

        nodeInterfaceList.add(remoteNode);
      }

      Collections.sort(nodeInterfaceList, new NodeInterfaceComparator());

      NodeInterface tempPred = nodeInterfaceList.get(nodeInterfaceList.size() - 1);
      NodeInterface tempSucc = nodeInterfaceList.get(0);
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

      // Link the ring
      tempPred.setSuccessor(this);
      tempSucc.setPredecessor(this);
      setSuccessor(tempSucc);
      setPredecessor(tempPred);

      ArrayList<NodeInterface> nodeList = getAllNodes();
      buildFingerTable(nodeList);
    } catch (Exception e) {
      System.err.println("Exception: " + e);
    }
  }

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
          predecessor.getPredecessor().removeLocal(key);
          successor.putLocal(key, storage.get(key));
          successor.getSuccessor().putLocal(key, storage.get(key));
        }
      }

      ArrayList<NodeInterface> nodeList = getAllNodes();
      nodeList.remove(nodeList.indexOf(this));

      // Exit the ring
      successor.setPredecessor(predecessor);
      predecessor.setSuccessor(successor);
      predecessor = this;
      successor = this;

      // Update all nodes' finger table
      for (NodeInterface node : nodeList) {
        node.buildFingerTable(nodeList);
      }
    } catch (RemoteException e) {
      System.err.println("RemoteException: " + e);
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
      System.err.println("RemoteException: " + e);
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
      targetNode.putLocal(key, value);

      // Back up replicas in predecessor and successor
      targetNode.getPredecessor().putLocal(key, value);
      targetNode.getSuccessor().putLocal(key, value);
    } catch (RemoteException e) {
      System.err.println("RemoteException: " + e);
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
    } catch (RemoteException e) {
      System.err.println("RemoteException: " + e);
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
    } catch (RemoteException e) {
      System.err.println("RemoteException: " + e);
    }

    return owners;
  }

  /* NodeInterface Implementation */

  @Override
  public void buildFingerTable(ArrayList<NodeInterface> nodeList) throws RemoteException {
    Collections.sort(nodeList, new NodeInterfaceComparator());
    ArrayList<NodeInterface> fingerTable = new ArrayList<>();

    int totalSpace = (int)Math.pow(2, HASH_BIT);

    try {
      String maxHashedId = nodeList.get(nodeList.size() - 1).getHashedId();

      for (int i = 0; i < HASH_BIT; i++) {
        int tempRes = (Integer.parseInt(getHashedId()) + (int)Math.pow(2, i)) % totalSpace;
        NodeInterface candidate;

        if (tempRes > Integer.parseInt(maxHashedId)) {
          candidate = nodeList.get(0);
        } else {
          int j = 0;
          while (Integer.parseInt(nodeList.get(j).getHashedId()) < tempRes) {
            j++;
          }
          candidate = nodeList.get(j);
        }
        fingerTable.add(candidate);
      }
    } catch (RemoteException e) {
      System.err.println("RemoteException: " + e);
    }

    this.fingerTable = fingerTable;
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
  public ArrayList<NodeInterface> getFingerTable() throws RemoteException {
    return this.fingerTable;
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
    String predHashedId = predecessor.getHashedId();
    String succHashedId = successor.getHashedId();

    if (ConsistentHashing.isHashedIdBetween(hashedId, predHashedId, this.hashedId)) {
      return this;
    } else if (ConsistentHashing.isHashedIdBetween(hashedId, this.hashedId, succHashedId)) {
      return successor;
    } else {
      // Largest finger entry <= k (i.e. hashedId)
      NodeInterface candidate = null;
      // Largest finger entry
      NodeInterface maxCandidate = null;

      for (int i = 0; i < fingerTable.size(); i++) {
        String curHashedId = fingerTable.get(i).getHashedId();

        if (maxCandidate == null || Integer.parseInt(maxCandidate.getHashedId()) < Integer.parseInt(curHashedId)) {
          maxCandidate = fingerTable.get(i);
        }

        if (Integer.parseInt(curHashedId) > Integer.parseInt(hashedId)) {
          continue;
        } else if (Integer.parseInt(curHashedId) == Integer.parseInt(hashedId)) {
          return fingerTable.get(i);
        } else if (candidate == null) {
          candidate = fingerTable.get(i);
        } else if (Integer.parseInt(candidate.getHashedId()) < Integer.parseInt(curHashedId)) {
          candidate = fingerTable.get(i);
        }
      }

      // Continue searching in largest finger entry <= k (i.e. hashedId) OR largest finger entry
      if (candidate != null) {
        return candidate.findNodeByHashedId(hashedId);
      } else {
        return maxCandidate.findNodeByHashedId(hashedId);
      }
    }
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

  public class NodeInterfaceComparator implements Comparator<NodeInterface> {
    @Override
    public int compare(NodeInterface a, NodeInterface b) {
      try {
        return Integer.parseInt(a.getHashedId()) - Integer.parseInt(b.getHashedId());
      } catch (RemoteException e) {
        System.err.println("RemoteException: " + e);
        return -1;
      }
    }
  }
}


