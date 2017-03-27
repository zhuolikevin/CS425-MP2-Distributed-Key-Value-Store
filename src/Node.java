import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class Node<K, V> extends UnicastRemoteObject implements DHT, NodeInterface {
  Node() throws RemoteException {
    super();
  }

  public String query(String search) throws RemoteException {
    String result;
    if (search.equals("Reflection in Java"))
      result = "Found";
    else
      result = "Not Found";

    return result;
  }
}
