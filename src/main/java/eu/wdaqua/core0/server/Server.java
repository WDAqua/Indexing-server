package eu.wdaqua.core0.server;
import java.net.*;

import org.apache.commons.math3.linear.OpenMapRealMatrix;

import dk.ange.octave.type.OctaveObject;
import eu.wdaqua.core0.connection.Connection;

import java.io.*;
 
public class Server {
	private static boolean acceptMore = true;
    public static void main(String[] args) throws IOException, ClassNotFoundException {
    	
    	try {
        	System.out.println("Server started!");
        	System.out.println("Indexing starts ...");
        	Index i = new Index();
        	i.index();
        	System.out.println("\nIndexing finished!");
        	System.out.println("I'm ready!");
        	
            ServerSocket serverSocket = new ServerSocket(1222);
            while (acceptMore) {
                Socket socket = serverSocket.accept();
                InputStream is =socket.getInputStream();
				ObjectInputStream ois = new ObjectInputStream(is);
				String[] URI = (String[])ois.readObject();
				OutputStream os;
				os = socket.getOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(os);
				try {
					System.out.println("Computing");
					Connection result = i.get(URI);
		        	out.writeObject(result);
				} catch (IllegalArgumentException e){
					System.out.println(e);
					out.writeObject(e);
				} finally {
					System.out.println("Sent to client!");
		        	out.close();
					is.close();
					socket.close();
				}
            }
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port");
            System.out.println(e.getMessage());
        }
    }
}
