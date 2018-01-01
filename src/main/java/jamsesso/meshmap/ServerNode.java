package jamsesso.meshmap;

import java.io.File;
import java.net.InetSocketAddress;

public class ServerNode {
  public static void main(String[] args) throws Exception {
    Node self = new Node(new InetSocketAddress("127.0.0.1", Integer.parseInt(args[0])));

    try (MeshMapCluster cluster = new LocalMeshMapCluster(self, new File("cluster"));
         MeshMap<Object, Object> map = cluster.join()) {
      System.out.println("Joined cluster: " + self);

      while (true) {
        Thread.sleep(10_000);
        System.out.println("Local map size: " + map.size());
      }
    }
  }
}
