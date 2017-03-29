import java.rmi.Remote;
import java.rmi.RemoteException;

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

//  /**
//   * Find the corresponding node with a hashed id
//   * @param hashedId
//   * @return a node where the key with this `hashedId` should be stored
//   * @throws RemoteException
//   */
//  public NodeInterface<K, V> findNodeByHashedId(String hashedId) throws RemoteException;
}
