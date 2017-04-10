import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.TimerTask;

public class HeartBeater extends TimerTask {
  protected NodeInterface thisNode;
  protected NodeInterface remoteNode;
  protected String remoteNodeId;
  private NodeInterface detectedPred;
  private NodeInterface detectedSucc;
//  private int membershipTableSize;
//  private int membershipTableSizeForCheck;
  private HashMap<String, String> localStorage;

  public HeartBeater(NodeInterface thisNode, NodeInterface remoteNode) throws Exception {
    this.thisNode = thisNode;
    this.remoteNode = remoteNode;
    this.remoteNodeId = remoteNode.getHashedId();
    this.detectedPred = thisNode;
    this.detectedSucc = thisNode;
//    this.membershipTableSize = thisNode.getMembershipTable().size();
//    this.membershipTableSizeForCheck = thisNode.getMembershipTable().size();
    this.localStorage = thisNode.getLocalStorage();
  }

  @Override
  public void run() {
    try {
      // Periodically get remote node hashedID, if can not get(exception), then it has failed
      remoteNode.getHashedId();
    } catch (RemoteException e) {
//      System.err.println(remoteNodeId + " failed");

      // Remove heart beat task
      try {
        thisNode.removeHeartBeat(remoteNodeId);
      } catch (Exception removeHeartBeatE) {
        System.err.println("[Remove HeartBeat Exception]" + removeHeartBeatE);
      }

//      // Set recovery flag
//      try {
//        thisNode.setRecoverStatus(true);
//      } catch (Exception setRecoverE) {
//        System.err.println("[Set Recover Status Exception]" + setRecoverE);
//      }

      // Remove from membership
      try {
        thisNode.removeMembership(Integer.parseInt(remoteNodeId));
      } catch (Exception removeMemE) {
        System.err.println("[Remove Member Exception]" + removeMemE);
      }

      try {
        Thread.sleep(1000);
      } catch (Exception interruptE) {
        System.err.println("[Recover Delay Exception]" + interruptE);
      }

      // Find predecessor and successor of detected node, repair the ring
      try {
        ArrayList<Integer> membershipList = new ArrayList<>(thisNode.getMembershipTable().keySet());
        Collections.sort(membershipList);

        int predHashedIdValue = membershipList.get(membershipList.size() - 1);
        int succHashedIdValue = membershipList.get(0);

        if (ConsistentHashing.isHashedIdBetween(
          remoteNodeId,
          String.valueOf(membershipList.get(membershipList.size() - 1)),
          String.valueOf(membershipList.get(0))
        )) {
          predHashedIdValue = membershipList.get(membershipList.size() - 1);
          succHashedIdValue = membershipList.get(0);
        } else {
          for (int i = 0; i < membershipList.size() - 1; i++) {
            if (ConsistentHashing.isHashedIdBetween(
              remoteNodeId,
              String.valueOf(membershipList.get(i)),
              String.valueOf(membershipList.get(i + 1))
            )) {
              predHashedIdValue = membershipList.get(i);
              succHashedIdValue = membershipList.get(i + 1);
              break;
            }
          }
        }

        this.detectedPred = thisNode.getMembershipTable().get(predHashedIdValue);
        this.detectedSucc = thisNode.getMembershipTable().get(succHashedIdValue);

        // Rebuild ring
        detectedPred.setSuccessor(detectedSucc);
        detectedSucc.setPredecessor(detectedPred);

      } catch (Exception repairRingE) {
        System.err.println("[Repair Ring Exception]" + repairRingE);
      }

      boolean stabled;
      do {
        stabled = true;
        try {
          Thread.sleep(1000);
        } catch (Exception interruptE) {
          System.err.println("[Duplicate Key Delay Exception]" + interruptE);
        }

        try {
          this.localStorage = thisNode.getLocalStorage();
        } catch (Exception getStorageE) {
          System.err.println("[Get Local Storage Exception]" + getStorageE);
        }

        // Duplicate lost keys for detected node
        for (String key : localStorage.keySet()) {
          try {
            String value = localStorage.get(key);
            String keyHashedId = ConsistentHashing.generateHashedId(key, (int)Math.pow(2, Node.HASH_BIT));
            NodeInterface keyHashedNode = thisNode.findNodeByHashedId(keyHashedId);

            if (!keyHashedNode.getLocalStorage().containsKey(key)) {
              keyHashedNode.putLocal(key, value);
            }
            if (!keyHashedNode.getPredecessor().getLocalStorage().containsKey(key)) {
              keyHashedNode.getPredecessor().putLocal(key, value);
            }
            if (!keyHashedNode.getSuccessor().getLocalStorage().containsKey(key)) {
              keyHashedNode.getSuccessor().putLocal(key, value);
            }
          } catch (Exception duplicatKeyE) {
            stabled = false;
            System.err.println("[Duplicate Key Exception]" + duplicatKeyE);
            continue;
          }
        }
      } while (stabled);

//      // Reset recovery flag
//      try {
//        thisNode.setRecoverStatus(false);
//      } catch (Exception resetRecoverE) {
//        System.err.println("[Reset Recover Status Exception]" + resetRecoverE);
//      }
    }
  }
}
