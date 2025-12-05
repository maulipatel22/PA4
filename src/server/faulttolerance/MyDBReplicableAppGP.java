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
package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

/**
 * GigaPaxos-based fault-tolerant database application using Cassandra.
 * Implements bounded log checkpointing (Option A).
 */
public class MyDBReplicableAppGP implements Replicable {

    public static final int SLEEP = 1000;
    public static final int MAX_LOG_SIZE = 400;

    private Cluster cluster;
    private Session session;
    private String keyspace;

    // Bounded log of executed requests for checkpointing
    private final Deque<String> executedRequests = new ArrayDeque<>();

    /**
     * Constructor used by GigaPaxos reflection.
     *
     * @param args args[0] = keyspace name
     * @throws IOException
     */
    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0)
            throw new IllegalArgumentException("Missing keyspace argument");

        this.keyspace = args[0];

        // Connect to Cassandra on localhost default port
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect();

        // Ensure keyspace exists
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace +
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor':1};");

        // Use keyspace
        session.execute("USE " + keyspace + ";");

        // Ensure default table exists
        session.execute("CREATE TABLE IF NOT EXISTS default_table (" +
                "key int PRIMARY KEY, " +
                "value int);");
    }

    /**
     * Execute a GigaPaxos request (client-facing version).
     */
    @Override
    public boolean execute(Request request, boolean b) {
        return execute(request);
    }

    /**
     * Execute a GigaPaxos request.
     */
    @Override
    public boolean execute(Request request) {
        if (!(request instanceof RequestPacket)) {
            throw new IllegalArgumentException("Unsupported request type");
        }

        RequestPacket rp = (RequestPacket) request;
        String command = rp.getCommand();

        // Log the command for checkpointing
        synchronized (executedRequests) {
            if (executedRequests.size() >= MAX_LOG_SIZE) {
                executedRequests.removeFirst();
            }
            executedRequests.addLast(command);
        }

        // Parse command: expecting "insert key value" or "update key value"
        String[] tokens = command.trim().split("\\s+");
        if (tokens.length < 2) {
            throw new IllegalArgumentException("Malformed command: " + command);
        }

        String op = tokens[0].toLowerCase();
        int key = Integer.parseInt(tokens[1]);
        int value = tokens.length >= 3 ? Integer.parseInt(tokens[2]) : 0;

        switch (op) {
            case "insert":
                session.execute("INSERT INTO default_table (key, value) VALUES (?, ?) IF NOT EXISTS",
                        key, value);
                break;
            case "update":
                session.execute("UPDATE default_table SET value = ? WHERE key = ?",
                        value, key);
                break;
            default:
                throw new IllegalArgumentException("Unknown operation: " + op);
        }

        return true;
    }

    /**
     * Checkpoint current executed requests into a string.
     */
    @Override
    public String checkpoint(String s) {
        StringBuilder sb = new StringBuilder();
        synchronized (executedRequests) {
            for (String cmd : executedRequests) {
                sb.append(cmd).append("\n");
            }
        }
        return sb.toString();
    }

    /**
     * Restore state from checkpoint string.
     */
    @Override
    public boolean restore(String s, String s1) {
        // Clear current table
        session.execute("TRUNCATE default_table;");

        // Clear current log
        synchronized (executedRequests) {
            executedRequests.clear();

            if (s1 != null && !s1.isEmpty()) {
                String[] commands = s1.split("\n");
                for (String cmd : commands) {
                    if (!cmd.trim().isEmpty()) {
                        execute(new RequestPacket(cmd, false));
                    }
                }
            }
        }

        return true;
    }

    /**
     * Convert string to RequestPacket.
     */
    @Override
    public Request getRequest(String s) throws RequestParseException {
        return new RequestPacket(s, false);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>();
    }
}
