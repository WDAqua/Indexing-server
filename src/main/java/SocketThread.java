import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

public class SocketThread implements Runnable {

        private Socket socket;

        public SocketThread(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {     
        	OutputStream os;
			try {
				InputStream is =socket.getInputStream();
				ObjectInputStream ois = new ObjectInputStream(is);
				String[] to = (String[])ois.readObject();
				System.out.println("Arrived:"+ to.toString());
				
				os = socket.getOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(os);                   
	            int in=123;
	        	out.writeObject(in);
	        	out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        }
    }
