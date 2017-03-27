import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class StorageController {
  private Node<String, String> node;

  private String[] addressBook = { "127.0.0.1 01", "127.0.0.1 02" };

  StorageController(String vmId) {
    try {
      node = new Node<>(vmId);
      ArrayList<String> addressList = new ArrayList<>(Arrays.asList(addressBook));
      for (int i = 0; i < addressList.size(); i++) {
        String remoteIp = addressList.get(i).split(" ")[0];
        String remoteId = addressList.get(i).split(" ")[1];
        if (remoteId.equals(vmId))
          continue;
        node.join(remoteIp, remoteId);
      }
      userInterface();
    } catch (RemoteException e) {
      System.err.println("RemoteException: " + e);
    }
  }

  private void userInterface() {
    Scanner scan = new Scanner(System.in);
    String input = scan.nextLine();
    while(!input.equals("exit")) {
      try {
        if (input.equals("GET")) {
          System.out.println(node.query());
        }
      } catch (RemoteException e) {
        System.err.println("RemoteException: " + e);
      }
    }
  }

  public static void main(String[] args) {
    if (args.length == 1) {
      String vmId = args[0];
      new StorageController(vmId);
    } else {
      System.err.println("Incorrect arguments!");
      System.err.println("Expected arguments: [vm ID]");
    }
  }
}
