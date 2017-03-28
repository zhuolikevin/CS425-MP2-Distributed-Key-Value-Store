import java.io.BufferedReader;
import java.io.FileReader;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Scanner;

public class StorageController {
  private Node<String, String> node;

  StorageController(String vmId) {
    try {
      // Init node
      node = new Node<>(vmId);
      Thread.sleep(500);

      // Read address book
      BufferedReader br = new BufferedReader(new FileReader("../res/address.txt"));
      String line = br.readLine();
      ArrayList<String> addressList = new ArrayList<>();
      while (line != null) {
        addressList.add(line);
        line = br.readLine();
      }
      br.close();
      node.setupRing(addressList);

      // User input
      userConsole();
    } catch (Exception e) {
      System.err.println("Exception: " + e);
    }
  }

  private void userConsole() {
    Scanner scan = new Scanner(System.in);
    String input = scan.nextLine();
    while(!input.equals("exit")) {
      try {
        String[] inputs = input.split(" ");
        switch (inputs[0]) {
          case "HASH":
            System.err.println(node.getName() + ":" + node.getHashedId());
            break;
          case "PRED":
            System.err.println(node.getPredecessor().getName() + ":" + node.getPredecessor().getHashedId());
            break;
          case "SUCC":
            System.err.println(node.getSuccessor().getName() + ":" + node.getSuccessor().getHashedId());
        }
      } catch (RemoteException e) {
        System.err.println("RemoteException: " + e);
      }
      input = scan.nextLine();
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
