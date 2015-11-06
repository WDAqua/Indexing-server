import java.net.*;

import org.apache.commons.math3.linear.OpenMapRealMatrix;

import dk.ange.octave.type.OctaveObject;

import java.io.*;
 
public class Server2 {
	private static boolean acceptMore = true;
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        try {
        	System.out.println("Server started!");
        	Index i = new Index();
        	i.index();
        	System.out.println("Indexing finished!");
        	System.out.println("I'm ready!");
        	
            ServerSocket serverSocket = new ServerSocket(1112);
            while (acceptMore) {
                Socket socket = serverSocket.accept();
                //new Thread(new SocketThread(socket)).start();
                InputStream is =socket.getInputStream();
				ObjectInputStream ois = new ObjectInputStream(is);
               
				String[] URI = (String[])ois.readObject();
				//System.out.println("Arrived:"+ URI[1].toString());
				
				long startTime = System.currentTimeMillis();
				
				
				System.out.println("Computing");
				OpenMapRealMatrix result = i.get(URI);
				long estimatedTime = System.currentTimeMillis() - startTime;
				System.out.println("Computed!: " + estimatedTime);
				
				OutputStream os;
				os = socket.getOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(os);
				System.out.println("Sending to client");
	        	out.writeObject(result);
	        	System.out.println("Sent to client!");
	        	out.close();
	        	is.close();
	        	socket.close();
            }
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                +  "? or listening for a connection");
            System.out.println(e.getMessage());
        }
    }
    
    
}

