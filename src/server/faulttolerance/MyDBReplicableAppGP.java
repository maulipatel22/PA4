package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.*;
import java.util.*;

public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;
    private static final int MAX_LOG_SIZE = 400; 
    private final Session session;
    private final Cluster cluster;
    private final String keyspace;

    private final LinkedList<String> opLog = new LinkedList<>();

    private final File paxosLogDir = new File("paxos_logs");

    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0) {
            throw new IllegalArgumentException("Expected args[0] = keyspace");
        }
        this.keyspace = args[0];
        if (!paxosLogDir.exists()) paxosLogDir.mkdirs();

        this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        this.session = cluster.connect(this.keyspace);
        File latest = new File(paxosLogDir, "checkpoint_" + keyspace + ".chk");
        if (latest.exists()) {
            try {
                String chk = readFileFully(latest);
                if (chk != null && !chk.isEmpty()) {
                    restore(chk, "");
                }
            } catch (Exception e) {
                System.err.println("Warning: failed to auto-restore checkpoint for " + keyspace + ": " + e);
            }
        }
    }

    private String extractRequestString(Request request) {
        if (request == null) return "";
        try {
            try {
                java.lang.reflect.Method m = request.getClass().getMethod("getRequestValue");
                Object val = m.invoke(request);
                if (val != null) return val.toString();
            } catch (NoSuchMethodException nsme) {
            }
            return request.toString();
        } catch (Exception e) {
            return request.toString();
        }
    }

    @Override
    public boolean execute(Request request, boolean b) {
        return execute(request);
    }
    @Override
    public boolean execute(Request request) {
        String cmd = extractRequestString(request);
        if (cmd == null || cmd.trim().isEmpty()) return false;

        cmd = cmd.trim();
        try {
            session.execute(cmd);
            synchronized (opLog) {
                opLog.add(cmd);
                if (opLog.size() > MAX_LOG_SIZE) {
                    String checkpointString = String.join("\n", opLog);
                    writeCheckpointToDisk(checkpointString);
                    opLog.clear();
                }
            }
            return true;
        } catch (Exception e) {
            System.err.println("MyDBReplicableAppGP.execute: failed to execute CQL -> " + cmd);
            e.printStackTrace();
            return false;
        }
    }
    @Override
    public String checkpoint(String s) {
        try {
            final String chk;
            synchronized (opLog) {
                chk = String.join("\n", opLog);
            }
            writeCheckpointToDisk(chk);
            return chk == null ? "" : chk;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    @Override
    public boolean restore(String s, String s1) {
        if (s == null) s = "";
        String[] lines = s.split("\n");
        try {
            for (String line : lines) {
                line = line.trim();
                if (line.isEmpty()) continue;
                session.execute(line);
            }
            return true;
        } catch (Exception e) {
            System.err.println("MyDBReplicableAppGP.restore: failed to restore checkpoint");
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Request getRequest(String s) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>();
    }

    private void writeCheckpointToDisk(String content) {
        try {
            File f = new File(paxosLogDir, "checkpoint_" + keyspace + ".chk");
            try (FileWriter fw = new FileWriter(f, false)) {
                fw.write(content == null ? "" : content);
            }
        } catch (Exception e) {
            System.err.println("Failed to write checkpoint file: " + e);
        }
    }

    private String readFileFully(File f) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
        }
        return sb.toString();
    }
}
