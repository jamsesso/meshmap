package jamsesso.meshmap.examples;

import jamsesso.meshmap.LocalMeshMapCluster;
import jamsesso.meshmap.MeshMap;
import jamsesso.meshmap.Node;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Scanner;

import static java.lang.System.in;
import static java.lang.System.out;

public class InteractiveNode {
  private final static Scanner scanner = new Scanner(in);

  public static void main(String[] args) throws Exception {
    int port = Integer.parseInt(args[0]);
    String directory = args[1];
    Node self = new Node(new InetSocketAddress("127.0.0.1", port));

    try (LocalMeshMapCluster cluster = new LocalMeshMapCluster(self, new File("cluster/" + directory));
         MeshMap<String, String> map = cluster.join()) {
      boolean running = true;

      do {
        out.println("Menu:");
        out.println(" 1 - add a key/value pair");
        out.println(" 2 - get a value");
        out.println(" 3 - remove a value");
        out.println(" 4 - get map size");
        out.println(" 5 - get all keys");
        out.println(" 6 - quit");
        out.print("Choose an option: ");

        int option = scanner.nextInt();
        scanner.nextLine(); // Consume the new line character.

        switch (option) {
          case 1:
            addPair(map);
            break;
          case 2:
            getValue(map);
            break;
          case 3:
            removeValue(map);
            break;
          case 4:
            getSize(map);
            break;
          case 5:
            getKeys(map);
            break;
          default:
            running = false;
            break;
        }
      }
      while(running);
    }
  }

  private static void addPair(MeshMap<String, String> map) {
    out.print("Key: ");
    String key = scanner.nextLine();
    out.print("Value: ");
    String value = scanner.nextLine();

    map.put(key, value);
    out.println("OK");
  }

  private static void getValue(MeshMap<String, String> map) {
    out.print("Search for key: ");
    String key = scanner.nextLine();
    String value = map.get(key);

    if (value == null) {
      out.println("(not found)");
    }
    else {
      out.println(value);
    }
  }

  private static void getKeys(MeshMap<String, String> map) {
    out.println("All keys: " + Arrays.toString(map.keySet().toArray()));
  }

  private static void getSize(MeshMap<String, String> map) {
    out.println("Size: " + map.size());
  }

  private static void removeValue(MeshMap<String, String> map) {
    out.print("Key to remove: ");
    String key = scanner.nextLine();
    map.remove(key);
    out.println("OK");
  }
}
