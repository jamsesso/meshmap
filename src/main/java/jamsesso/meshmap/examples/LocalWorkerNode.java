package jamsesso.meshmap.examples;

import jamsesso.meshmap.LocalMeshMapCluster;
import jamsesso.meshmap.MeshMap;
import jamsesso.meshmap.Node;

import java.io.File;
import java.net.InetSocketAddress;

import static java.lang.System.in;
import static java.lang.System.out;

public class LocalWorkerNode {
  public static void main(String[] args) throws Exception {
    // Get input from arguments.
    int port = Integer.parseInt(args[0]);
    String directory = args[1];

    // Set up cluster and wait. Enter key kills the server.
    Node self = new Node(new InetSocketAddress("127.0.0.1", port));

    try (LocalMeshMapCluster cluster = new LocalMeshMapCluster(self, new File("cluster/" + directory));
         MeshMap<String, String> map = cluster.join()) {
      in.read();
      out.println("Node is going down...");
    }
  }
}
