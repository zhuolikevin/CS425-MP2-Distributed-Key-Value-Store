import java.rmi.Remote;
import java.rmi.RemoteException;

public interface NodeInterface<K, V> extends Remote {
//  /**
//   * Get the hashed id of the node
//   * @return hashed id
//   * @throws RemoteException
//   */
//  public String getHashedId() throws RemoteException;
//
//  /**
//   * Get successor of the node
//   * @return the successor node
//   * @throws RemoteException
//   */
//  public NodeInterface<K, V> getSuccessor() throws RemoteException;
//
//  /**
//   * Get predecessor of the node
//   * @return the predecessor node
//   * @throws RemoteException
//   */
//  public NodeInterface<K, V> getPredecessor() throws RemoteException;
//
//  /**
//   * Set the successor of the node to be `successor`
//   * @param successor
//   * @throws RemoteException
//   */
//  public void setSuccessor(NodeInterface<K, V> successor) throws RemoteException;
//
//  /**
//   * Set the predecessor of the node to be `predecessor`
//   * @param predecessor
//   * @throws RemoteException
//   */
//  public void setPredecessor(NodeInterface<K, V> predecessor) throws RemoteException;
//
//  /**
//   * Find the corresponding node with a hashed id
//   * @param hashedId
//   * @return a node where the key with this `hashedId` should be stored
//   * @throws RemoteException
//   */
//  public NodeInterface<K, V> findNodeByHashedId(String hashedId) throws RemoteException;

  // For test
  public String query() throws RemoteException;
}
