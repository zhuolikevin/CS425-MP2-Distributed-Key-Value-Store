import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;

public interface NodeInterface extends Remote {
  /**
   * Get name of the node
   * @return node name
   * @throws RemoteException
   */
  String getName() throws RemoteException;
  /**
   * Get the hashed id of the node
   * @return hashed id
   * @throws RemoteException
   */
  String getHashedId() throws RemoteException;

  boolean getRecoverStatus() throws RemoteException;

  void setRecoverStatus(boolean flag) throws RemoteException;

  /**
   * Get membership table of the node
   * @return membership table in HashMap
   * @throws RemoteException
   */
  HashMap<Integer, NodeInterface> getMembershipTable() throws RemoteException;

  void removeMembership(Integer hashedIdValue) throws RemoteException;

  /**
   * Initialize or update membership table of the current node.
   * @param nodeList
   * @throws RemoteException
   */
  void buildMembershipTable(ArrayList<NodeInterface> nodeList) throws RemoteException;

  void updateMembershipTable() throws RemoteException;

  /**
   * Get local storage of the node
   * @return local storage in HashMap
   * @throws RemoteException
   */
  HashMap<String, String> getLocalStorage() throws RemoteException;

  /**
   * Get successor of the node
   * @return the successor node
   * @throws RemoteException
   */
  NodeInterface getSuccessor() throws RemoteException;

  /**
   * Get predecessor of the node
   * @return the predecessor node
   * @throws RemoteException
   */
  NodeInterface getPredecessor() throws RemoteException;

  /**
   * Set the successor of the node to be `successor`
   * @param successor
   * @throws RemoteException
   */
  void setSuccessor(NodeInterface successor) throws RemoteException;

  /**
   * Set the predecessor of the node to be `predecessor`
   * @param predecessor
   * @throws RemoteException
   */
  void setPredecessor(NodeInterface predecessor) throws RemoteException;

  /**
   * Find the corresponding node with a hashed id
   * @param hashedId
   * @return a node where the key with this `hashedId` should be stored
   * @throws RemoteException
   */
  NodeInterface findNodeByHashedId(String hashedId) throws RemoteException;

  /**
   * Store key-value pair in this node locally
   * @param key
   * @param value
   * @throws RemoteException
   */
  void putLocal(String key, String value) throws RemoteException;

  /**
   * Get value in this node locally with key
   * @param key
   * @return value associated with the key
   * @throws RemoteException
   */
  String getLocal(String key) throws RemoteException;

  /**
   * Remove a key in this node locally
   * @param key
   * @throws RemoteException
   */
  void removeLocal(String key) throws RemoteException;

  void setupHeartBeat(String hashedId) throws RemoteException;

  void removeHeartBeat(String hashedId) throws RemoteException;
}
