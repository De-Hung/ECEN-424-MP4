import java.net.*;
import java.util.*;

public class Talker {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java Talker <talkerPort> <listenerPort> [messageString]");
            return;
        }

        int talkerPort;
        int listenerPort;
        try {
            talkerPort = Integer.parseInt(args[0]);
            listenerPort = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.out.println("Error: Ports must be valid integers.");
            return;
        }

        // Accept string from command line or prompts user to input
        String inputString;
        if (args.length >= 3) {
            inputString = args[2];
        } else {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Enter string to send (up to 50 chars): ");
            inputString = scanner.nextLine();
            scanner.close();
        }

        // Validate string length (50 characters max)
        if (inputString == null || inputString.isEmpty()) {
            System.out.println("Error: Input string cannot be empty.");
            return;
        }
        if (inputString.length() > 50) {
            System.out.println("Warning: String exceeds 50 characters. Truncating to 50 characters.");
            inputString = inputString.substring(0, 50);
        }

        // Split string into chunks of 10 characters max (5 chunks max)
        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < inputString.length(); i += 10) {
            chunks.add(inputString.substring(i, Math.min(i + 10, inputString.length())));
        }
        int totalMessages = chunks.size();

        try (DatagramSocket socket = new DatagramSocket(talkerPort)) {
            System.out.println("Talker listening on port " + talkerPort + " for Listener connection...");

            // 1. Wait for Listener to request UDP connection
            byte[] buf = new byte[1024];
            DatagramPacket connectPacket = new DatagramPacket(buf, buf.length);
            socket.receive(connectPacket);

            String connectMsg = new String(connectPacket.getData(), 0, connectPacket.getLength()).trim();
            InetAddress listenerIP = connectPacket.getAddress();
            System.out.println("Received '" + connectMsg + "' from Listener at " + listenerIP.getHostAddress());

            // 2. Connect UDP socket to Listener: send messages & receive ACKs
            socket.connect(listenerIP, listenerPort);
            System.out.println("Connected to Listener at " + listenerIP.getHostAddress() + ":" + listenerPort);

            // Set 2-second timeout for S&W retransmission
            socket.setSoTimeout(2000);

            // Build frames
            String[] frames = new String[totalMessages + 1];
            frames[0] = "0:" + totalMessages;
            for (int i = 0; i < totalMessages; i++) {
                frames[i + 1] = (i + 1) + ":" + chunks.get(i);
            }

            // 3. S&W Transmission Loop
            int seqNum = 0;
            while (seqNum <= totalMessages) {
                byte[] sendData = frames[seqNum].getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, listenerIP, listenerPort);

                boolean acked = false;
                while (!acked) {
                    socket.send(sendPacket);
                    System.out.println("[SENT] Message ID: " + seqNum + " | Payload: \"" + frames[seqNum] + "\"");

                    try {
                        byte[] ackBuf = new byte[1024];
                        DatagramPacket ackPacket = new DatagramPacket(ackBuf, ackBuf.length);
                        socket.receive(ackPacket);

                        String ackStr = new String(ackPacket.getData(), 0, ackPacket.getLength()).trim();
                        if (ackStr.startsWith("ACK:")) {
                            int nextExpectedSeq = Integer.parseInt(ackStr.split(":")[1]);
                            System.out.println("[RECV] " + ackStr + " (Next expected ID: " + nextExpectedSeq + ")");

                            // Verify ACK matches next sequence number
                            if (nextExpectedSeq == seqNum + 1) {
                                acked = true;
                                seqNum++;
                            } else {
                                System.out.println("[WARN] Unexpected ACK ID. Waiting...");
                            }
                        }
                    } catch (SocketTimeoutException e) {
                        System.out.println("[TIMEOUT] No ACK for Message ID: " + seqNum + ". Retransmitting...");
                    }
                }
            }

            System.out.println("All messages sent and ACKed. Closing Talker.");
        } catch (Exception e) {
            System.err.println("Talker Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}