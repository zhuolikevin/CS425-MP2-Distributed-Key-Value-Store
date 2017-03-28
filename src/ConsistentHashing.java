import java.math.BigInteger;
import java.security.MessageDigest;

public class ConsistentHashing {
  public static String generateHashedId(String key, int space) {
    String hashedId = null;
    try {
      MessageDigest md = MessageDigest.getInstance("SHA1");
      byte[] result = md.digest(key.getBytes());
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < result.length; i++) {
        sb.append(Integer.toString(result[i]).substring(1));
      }
      BigInteger idValue = new BigInteger(sb.toString());
      BigInteger spaceValue = new BigInteger(String.valueOf(space));
      hashedId = String.valueOf(idValue.mod(spaceValue));
    } catch (Exception e) {
      System.err.println("Exception: " + e);
    }

    return hashedId;
  }

  public static boolean isHashedIdBetween(String hashedId, String from, String to) {
    int hashedIdValue = Integer.parseInt(hashedId);
    int fromValue = Integer.parseInt(from);
    int toValue = Integer.parseInt(to);

    if (fromValue < toValue) {
      return hashedIdValue > fromValue && hashedIdValue <= toValue;
    } else if (fromValue > toValue) {
      return hashedIdValue > fromValue || hashedIdValue <= toValue;
    } else {
      return true;
    }
  }
}
