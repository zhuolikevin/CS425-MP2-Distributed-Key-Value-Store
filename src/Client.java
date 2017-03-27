import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Created by Kevin on 3/25/17.
 */
public class Client {
  public static void main(String args[]) throws Exception {
//    NodeInterface<String, String> stub=(NodeInterface<String, String>) Naming.lookup("rmi://localhost:5000/sonoo");
//    System.out.println(stub.add(34,4));
    String value="Reflection in Java";
    try {
      Registry registry = LocateRegistry.getRegistry("127.0.0.1", 10000);
      NodeInterface<String, String> remoteNode = (NodeInterface<String, String>) registry.lookup("hello");
      System.out.println(remoteNode.query(value));
    } catch (Exception ae) {
      System.out.println(ae);
    }
  }
}
