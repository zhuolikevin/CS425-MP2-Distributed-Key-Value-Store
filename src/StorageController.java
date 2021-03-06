import java.io.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

public class StorageController {
  private Node node;

  private static final String RES_PREFIX = "../res/";

  StorageController(String vmId) {
    try {
      // Init node
      node = new Node(vmId);
      Thread.sleep(500);

      // Read address book
      BufferedReader br = new BufferedReader(new FileReader(RES_PREFIX + "address.txt"));
      String line = br.readLine();
      ArrayList<String> addressList = new ArrayList<>();
      while (line != null) {
        addressList.add(line);
        line = br.readLine();
      }
      br.close();
      node.setupChord(addressList);

      // User input
      userConsole();
    } catch (Exception e) {
      System.err.println("Exception15: " + e);
    }
  }

  private void userConsole() {
    Scanner scan = new Scanner(System.in);
    String input = scan.nextLine();
    while(!input.equals("EXIT")) {
      try {
        String[] inputs = input.split(" ");
        switch (inputs[0]) {
          /* Commands for debugging */
          case "ME":
            System.err.println(node.getName() + ":" + node.getHashedId());
            break;
          case "PRED":
            System.err.println(node.getPredecessor().getName() + ":" + node.getPredecessor().getHashedId());
            break;
          case "SUCC":
            System.err.println(node.getSuccessor().getName() + ":" + node.getSuccessor().getHashedId());
            break;
          case "ALL":
            ArrayList<NodeInterface> nodeList = node.getAllNodes();
            for (NodeInterface node : nodeList) {
              System.err.println(node.getName() + ":" + node.getHashedId());
            }
            break;
          case "RECOVER":
            System.err.println(node.getRecoverStatus());
            break;
          case "MEMBER":
            HashMap<Integer, NodeInterface> membershipTable = node.getMembershipTable();
            for (int hashedId : membershipTable.keySet()) {
              System.err.println(hashedId + ":" + membershipTable.get(hashedId).getName());
            }
            break;
          case "HB":
            HashMap<String, HeartBeater> heartBeaterTaskMap = node.getHeartBeaterTaskMap();
            for (String hashedId : heartBeaterTaskMap.keySet()) {
              System.err.println(hashedId + ":" + heartBeaterTaskMap.get(hashedId).remoteNode.getName());
            }
            break;
          case "HASH":
            if (inputs.length < 2) {
              System.err.println("Invalid command");
            } else {
              String key = inputs[1];
              String hashedId = ConsistentHashing.generateHashedId(key, (int) Math.pow(2, Node.HASH_BIT));
              System.err.println(hashedId);
            }
            break;
          /* Commands for mp requirements */
          case "SET":
            if (inputs.length < 3) {
              System.err.println("Invalid command");
            } else {
              String key = inputs[1];
              String value = inputs[2];
              node.put(key, value);
              System.out.println("SET OK");
            }
            break;
          case "GET":
            if (inputs.length < 2) {
              System.err.println("Invalid command");
            } else {
              String key = inputs[1];
              String value = node.get(key);
              if (value == null) {
                System.out.println("Not found");
              } else {
                System.out.println("Found: " + value);
              }
            }
            break;
          case "OWNERS":
            if (inputs.length < 2) {
              System.err.println("Invalid command");
            } else {
              String key = inputs[1];
              ArrayList<NodeInterface> owners = node.findOwners(key);
              ArrayList<String> ownerNames = new ArrayList<>();
              for (NodeInterface node : owners) {
                ownerNames.add(node.getName());
              }
              System.out.println(String.join(" ", ownerNames));
            }
            break;
          case "LIST_LOCAL":
            HashMap<String, String> localStorage = node.getLocalStorage();
            ArrayList<String> localKeys = new ArrayList<>(localStorage.keySet());
            Collections.sort(localKeys);
            for (String localKey : localKeys) {
              System.out.println(localKey);
            }
            System.out.println("END LIST");
            break;
          case "BATCH":
            if (inputs.length < 3) {
              System.err.println("Invalid command");
            } else {
              String inputFile = inputs[1];
              String outputFile = inputs[2];
              batchOperation(inputFile, outputFile);
            }
            break;
          default:
            System.err.println("Invalid command");
        }
      } catch (RemoteException e) {
        System.err.println("Exception16: " + e);
      }
      input = scan.nextLine();
    }
    node.leave();
    scan.close();
    System.exit(0);
  }

  private void batchOperation(String input, String output) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(RES_PREFIX + input));
      BufferedWriter bw = new BufferedWriter(new FileWriter(RES_PREFIX + output, true));
      PrintWriter pw = new PrintWriter(bw);

      String line = br.readLine();
      while (line != null) {
        String[] inputs = line.split(" ");
        switch (inputs[0]) {
          case "SET":
            String key = inputs[1];
            String value = inputs[2];
            node.put(key, value);
            pw.println("SET OK");
            break;
          case "GET":
            key = inputs[1];
            value = node.get(key);
            if (value == null) {
              pw.println("Not found");
            } else {
              pw.println("Found: " + value);
            }
            break;
          case "OWNERS":
            key = inputs[1];
            ArrayList<NodeInterface> owners = node.findOwners(key);
            ArrayList<String> ownerNames = new ArrayList<>();
            for (NodeInterface node : owners) {
              ownerNames.add(node.getName());
            }
            pw.println(String.join(" ", ownerNames));
            break;
          case "LIST_LOCAL":
            HashMap<String, String> localStorage = node.getLocalStorage();
            ArrayList<String> localKeys = new ArrayList<>(localStorage.keySet());
            Collections.sort(localKeys);
            for (String localKey : localKeys) {
              pw.println(localKey);
            }
            pw.println("END LIST");
            break;
          default:
            pw.println("Invalid command");
        }
        line = br.readLine();
      }
      pw.close();
    } catch (Exception e) {
      System.err.println("Exception17: " + e);
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
