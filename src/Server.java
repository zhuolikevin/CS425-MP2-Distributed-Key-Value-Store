import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by Kevin on 3/25/17.
 */
public class Server {
  public static void main(String args[]) throws Exception {
//    Node<String, String> stub = new Node<>();
//    Naming.rebind("rmi://localhost:5000/sonoo",stub);
    Registry registry;
//    try {
      registry = LocateRegistry.createRegistry(10000);
//    }
//    catch (Exception e) {
//      registry = LocateRegistry.getRegistry(10000);
//    }

    Node<String, String> curNode = new Node<>();
    registry.rebind("hello", curNode);
  }
}
