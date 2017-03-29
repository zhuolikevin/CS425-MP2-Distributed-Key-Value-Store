import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

public class Node extends UnicastRemoteObject implements DHT, NodeInterface {
  private String name;
  private String hashedId;
  private NodeInterface successor;
  private NodeInterface predecessor;
  private ArrayList<NodeInterface> fingerTable;
  private HashMap<String, String> storage;

  private static final int HASH_BIT = 7;

  Node(String vmId) throws RemoteException {
    this.name = "vm-" + vmId;
    this.hashedId = ConsistentHashing.generateHashedId(this.name, (int)Math.pow(2, HASH_BIT));
    this.successor = this;
    this.predecessor = this;
    this.fingerTable = new ArrayList<>();

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
      ArrayList<NodeInterface> nodeInterfaceList = new ArrayList<>();
      for (int i = 0; i < addressList.size(); i++) {
        String remoteIp = addressList.get(i).split(" ")[0];
        String remoteId = addressList.get(i).split(" ")[1];

        int remotePort = Integer.parseInt("100" + remoteId);
        String remoteName = "vm-" + remoteId;

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
      tempPred.setSuccessor(this);
      tempSucc.setPredecessor(this);
      setSuccessor(tempSucc);
      setPredecessor(tempPred);
    } catch (Exception e) {
      System.err.println("Exception: " + e);
    }
  }

  public void buildFingerTable() {
    ArrayList<NodeInterface> nodeList = getAllNodes();
    Collections.sort(nodeList, new NodeInterfaceComparator());

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
        this.fingerTable.add(candidate);
      }
    } catch (RemoteException e) {
      System.err.println("RemoteException: " + e);
    }
  }

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

  public ArrayList<NodeInterface> getFingerTable() {
    return this.fingerTable;
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


