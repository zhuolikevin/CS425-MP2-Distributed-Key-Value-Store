import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class Node<K, V> extends UnicastRemoteObject implements DHT, NodeInterface {
  private String name;
  private String hashedId;
  private NodeInterface<K, V> successor;
  private NodeInterface<K, V> predecessor;
  private HashMap<String, String> storage;
  private HashMap<String, NodeInterface<K, V>> fingerTable;

  Node(String vmId) throws RemoteException {
    this.name = "vm-" + vmId;
    int port = Integer.parseInt("100" + vmId);

    Registry registry;
    registry = LocateRegistry.createRegistry(port);
    registry.rebind(this.name, this);
  }

  public void join(String remoteIp, String remoteId) {
    int remotePort = Integer.parseInt("100" + remoteId);
    String remoteName = "vm-" + remoteId;
    try {
      Registry registry = LocateRegistry.getRegistry(remoteIp, remotePort);
      NodeInterface<String, String> remoteNode = (NodeInterface<String, String>) registry.lookup(remoteName);
      System.out.println(remoteNode.query());
    } catch (Exception e) {
      System.err.println("Exception: " + e);
    }
  }

  public String query() throws RemoteException {

    return this.name;
  }
}
