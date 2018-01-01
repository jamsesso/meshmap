package jamsesso.meshmap;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalMeshMapCluster implements MeshMapCluster, AutoCloseable {
  private final Node self;
  private final File directory;
  private MeshMapServer server;
  private MeshMap map;

  public LocalMeshMapCluster(Node self, File directory) {
    directory.mkdirs();

    if (!directory.isDirectory()) {
      throw new IllegalArgumentException("File passed to LocalMeshMapCluster must be a directory");
    }

    if (!directory.canRead() || !directory.canWrite()) {
      throw new IllegalArgumentException("Directory must be readable and writable");
    }

    this.self = self;
    this.directory = directory;
  }

  @Override
  public List<Node> getAllNodes() {
    return Stream.of(directory.listFiles())
      .filter(File::isFile)
      .map(File::getName)
      .map(Node::from)
      .sorted(Comparator.comparingInt(Node::getId))
      .collect(Collectors.toList());
  }

  @Override
  public <K, V> MeshMap<K, V> join() throws MeshMapException {
    if (this.map != null) {
      return (MeshMap<K, V>) this.map;
    }

    File file = new File(directory.getAbsolutePath() + File.separator + self.toString());

    try {
      boolean didCreateFile = file.createNewFile();

      if(!didCreateFile) {
        throw new MeshMapException("File could not be created: " + file.getName());
      }
    }
    catch (IOException e) {
      throw new MeshMapException("Unable to join cluster", e);
    }

    file.deleteOnExit();

    server = new MeshMapServer(this, self);
    MeshMapImpl<K, V> map = new MeshMapImpl<>(this, server, self);

    try {
      server.start(map);
      map.open();
    }
    catch(IOException e) {
      throw new MeshMapException("Unable to start the mesh map server", e);
    }

    server.broadcast(Message.HI);
    this.map = map;

    return map;
  }

  @Override
  public void close() throws Exception {
    File file = new File(directory.getAbsolutePath() + File.separator + self.toString());
    boolean didDeleteFile = file.delete();

    if (!didDeleteFile) {
      throw new MeshMapException("File could not be deleted: " + file.getName());
    }

    if (server != null) {
      server.broadcast(Message.BYE);
      server.close();
    }
  }
}
