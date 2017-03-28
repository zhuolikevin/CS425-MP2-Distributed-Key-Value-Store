import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class Node<K, V> extends UnicastRemoteObject implements DHT, NodeInterface {
  private String name;
  private String hashedId;
  private NodeInterface<K, V> successor;
  private NodeInterface<K, V> predecessor;
  private HashMap<String, String> storage;
  private HashMap<String, NodeInterface<K, V>> fingerTable;

  private static final int HASH_BIT = 7;

  Node(String vmId) throws RemoteException {
    this.name = "vm-" + vmId;
    this.hashedId = ConsistentHashing.generateHashedId(this.name, (int)Math.pow(2, HASH_BIT));
    this.successor = this;
    this.predecessor = this;

    Registry registry;
    int port = Integer.parseInt("100" + vmId);
    registry = LocateRegistry.createRegistry(port);
    registry.rebind(this.name, this);
  }

  /**
   * Set up Chord ring structure. Decide predecessor and successor of the node.
   * @param addressList
   */
  public void setupRing(ArrayList<String> addressList) {
    try {
      ArrayList<NodeInterface<String, String>> nodeInterfaceList = new ArrayList<>();
      for (int i = 0; i < addressList.size(); i++) {
        String remoteIp = addressList.get(i).split(" ")[0];
        String remoteId = addressList.get(i).split(" ")[1];

        int remotePort = Integer.parseInt("100" + remoteId);
        String remoteName = "vm-" + remoteId;

        if (remoteName.equals(this.name))
          continue;

        Registry registry = LocateRegistry.getRegistry(remoteIp, remotePort);
        NodeInterface<String, String> remoteNode = (NodeInterface<String, String>) registry.lookup(remoteName);

        nodeInterfaceList.add(remoteNode);
      }

      Collections.sort(nodeInterfaceList, new NodeInterfaceComparator());

      NodeInterface<String, String> tempPred = nodeInterfaceList.get(nodeInterfaceList.size() - 1);
      NodeInterface<String, String> tempSucc = nodeInterfaceList.get(0);
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
      tempPred.setSuccessor(this);
      tempSucc.setPredecessor(this);
      setSuccessor(tempSucc);
      setPredecessor(tempPred);
    } catch (Exception e) {
      System.err.println("Exception: " + e);
    }
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getHashedId() throws RemoteException {
    return this.hashedId;
  }

  @Override
  public NodeInterface<K, V> getSuccessor() throws RemoteException {
    return this.successor;
  }

  @Override
  public NodeInterface<K, V> getPredecessor() throws RemoteException {
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

  public class NodeInterfaceComparator implements Comparator<NodeInterface<String, String>> {
    @Override
    public int compare(NodeInterface<String, String> a, NodeInterface<String, String> b) {
      try {
        return Integer.parseInt(a.getHashedId()) - Integer.parseInt(b.getHashedId());
      } catch (RemoteException e) {
        System.err.println("RemoteException: " + e);
        return -1;
      }
    }
  }
}


