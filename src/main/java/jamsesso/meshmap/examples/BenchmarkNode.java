package jamsesso.meshmap.examples;

import jamsesso.meshmap.LocalMeshMapCluster;
import jamsesso.meshmap.MeshMap;
import jamsesso.meshmap.Node;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.UUID;

import static java.lang.System.out;

public class BenchmarkNode {
  private static final int COUNT = 100_000;

  public static void main(String[] args) throws Exception {
    // Get input from arguments.
    int port = Integer.parseInt(args[0]);
    String directory = args[1];

    // Generate data
    String[] keys = new String[COUNT];
    String[] values = new String[COUNT];

    for (int i = 0; i < COUNT; i++) {
      keys[i] = UUID.randomUUID().toString();
      values[i] = String.valueOf(i);
    }

    // Set up cluster and wait. Enter key kills the server.
    Node self = new Node(new InetSocketAddress("127.0.0.1", port));

    try (LocalMeshMapCluster cluster = new LocalMeshMapCluster(self, new File("cluster/" + directory));
         MeshMap<String, String> map = cluster.join()) {
      Timer.time("PUT", COUNT, i -> {
        map.put(keys[i], values[i]);

        if(i % 1_000 == 0) {
          out.println("Put " + i + " key/value pairs");
        }
      });

      map.clear();
    }
  }
}
