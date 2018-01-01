package jamsesso.meshmap;

import java.io.File;
import java.net.InetSocketAddress;

public class ClientNode {
  public static void main(String[] args) throws Exception {
    Node self = new Node(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[0])));

    try (MeshMapCluster cluster = new LocalMeshMapCluster(self, new File("cluster"));
         MeshMap<String, String> map = cluster.join()) {
      System.out.println("Joined cluster: " + self);

      map.put("name", "Sam Jesso");
      map.put("age", "23");
      map.put("status", "Married");
      map.put("salary", "80000");
      map.put("location", "Fredericton NB");

      Thread.sleep(10_000);

      System.out.println(map.get("name"));
      System.out.println(map.get("age"));
      System.out.println(map.get("status"));
      System.out.println(map.get("salary"));
      System.out.println(map.get("location"));
    }
  }
}
