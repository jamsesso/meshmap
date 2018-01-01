package jamsesso.meshmap;

import java.io.File;
import java.net.InetSocketAddress;

public class ClientNode {
  public static void main(String[] args) throws Exception {
    Node self = new Node(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[0])));

    try (MeshMapCluster cluster = new LocalMeshMapCluster(self, new File("cluster"));
         MeshMap<String, String> map = cluster.join()) {
      System.out.println("Joined cluster: " + self);

      map.clear();

      map.put("name", "Sam Jesso");
      map.put("github_profile", "http://github.com/jamsesso");
      map.put("profession", "Software Developer");

      System.out.println(map);

      System.out.println(map.get("name"));
      System.out.println(map.get("github_profile"));
      System.out.println(map.get("profession"));
    }
    finally {
      System.out.println("Node is leaving cluster");
    }
  }
}
