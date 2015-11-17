import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//Java wrapper around octave
import dk.ange.octave.OctaveEngine;
import dk.ange.octave.OctaveEngineFactory;
import dk.ange.octave.type.OctaveDouble;
//Compressed column storage implementation
import no.uib.cipr.matrix.io.MatrixVectorReader;
import no.uib.cipr.matrix.sparse.CompColMatrix;
//Sparse matrix implementation
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.jena.graph.Triple;
//For parsing RDF files
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.log4j.BasicConfigurator;

public class Index {
	
	
	String octavePath="/usr/local/bin/octave";
	String labels = "/Users/Dennis/Downloads/labels-en-uris_it.nt";
	String dump = "/Users/Dennis/Downloads/dump-it.nt";
	
	
	private HashMap<String,Integer> mapIn = new HashMap<String,Integer>();
	private ArrayList<String> mapOut = new ArrayList<String>();
	private HashMap<String,Integer> mapInRelation = new HashMap<String,Integer>();
	private ArrayList<String> mapOutRelation = new ArrayList<String>();
	private CompColMatrix matrixIndex;
	private OctaveEngine octave;

	Index(){
		OctaveEngineFactory factory = new OctaveEngineFactory();
		factory.setOctaveProgram(new File(octavePath));
		octave = factory.getScriptEngine();
	}
	
	public OpenMapRealMatrix get(String[] URI) throws IllegalArgumentException{
		String[] tmp= new String[URI.length];
		int k=0;
		long startTime = System.currentTimeMillis();
		for (String uri : URI){
			//System.out.println(uri);
			if (mapIn.containsKey(uri)){
				tmp[k] = mapIn.get(uri).toString();
			} else if (mapInRelation.containsKey(uri)){
				tmp[k] = ((Integer)(mapIn.size()+mapInRelation.get(uri))).toString();
			} else {
				throw new IllegalArgumentException("The URI "+ uri +" is not in the index!");
			}
			k++;
		}
		String indeces = String.join(", ", tmp);
		
		
        //Selects the submatrix containing the rows and columns contained in indeces
        //String func="ans=full(B(["+indeces+" ] , [ "+indeces+"]))";
        startTime = System.currentTimeMillis();
        octave.eval("C = full(B(["+indeces+" ] , [ "+indeces+"]));");
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("eval: "+estimatedTime);
        OctaveDouble ans = octave.get(OctaveDouble.class, "C");
        double[] a = ans.getData();
        estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Retrive: "+estimatedTime);
        //int[][] A=new int[URI.length][URI.length];
        OpenMapRealMatrix B = new  OpenMapRealMatrix(URI.length,URI.length);
        System.out.println("Length"+URI.length);
        for (int i=0; i<URI.length; i++){
                for (int j=0; j<URI.length; j++){
                        //A[i][j]=(int)a[j+URI.length*i];
                        if (a[j+URI.length*i]!=0){
                                B.setEntry(i, j, a[j+URI.length*i]);
                        }
                        //System.out.println(A[i][j]);
                }
        }
        //System.out.println(A.toString());
        return B;
	}
	
	
	public Integer get(String URI) throws IOException, ClassNotFoundException{
		return (Integer) mapIn.get(URI);
	}
	
	
	public String get(Integer i) throws IOException, ClassNotFoundException{
		return (String)mapOut.get(i);
	}
	
	public void index() throws IOException, ClassNotFoundException{
		//create the index folder
		File folder = new File("index/");
		if (!folder.exists()) folder.mkdir();
		//Parse the labels file
		//Using example: https://github.com/apache/jena/blob/master/jena-arq/src-examples/arq/examples/riot/ExRIOT_6.java
		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
        final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);
        // PipedRDFStream and PipedRDFIterator need to be on different threads
        ExecutorService executor = Executors.newSingleThreadExecutor();
        // Create a runnable for our parser thread
        Runnable parser = new Runnable() {
            @Override
            public void run() {
                // Call the parsing process.
                RDFDataMgr.parse(inputStream, labels);
            }
        };

        // Start the parser on another thread
        executor.submit(parser);
        Integer i=1;
        
        System.out.println("Parse the label dump ...");
        System.out.println("If this slows down, and does not come to an end, you probably need more RAM!");
        while (iter.hasNext()) {
            Triple next = iter.next();
            
            mapIn.put(next.getSubject().toString(),i);
	    	mapOut.add(next.getSubject().toString());
	    	i++;
	    	if(i % 100000 == 0) {
	 	    	System.out.println(i);
	    	}
        }
        System.out.println("Number resources: "+i);
        executor.shutdownNow();
        
        //Relation extraction
        iter = new PipedRDFIterator<Triple>();
        final PipedRDFStream<Triple> inputStream3 = new PipedTriplesStream(iter);
        // PipedRDFStream and PipedRDFIterator need to be on different threads
        executor = Executors.newSingleThreadExecutor();
        parser = new Runnable() {
            @Override
            public void run() {
                // Call the parsing process.
            	RDFDataMgr.parse(inputStream3, dump);
            }
        };
        executor.submit(parser);
        
        int r=1;
        System.out.println("Search for relations in the dump ...");
        while ( iter.hasNext()){
        	Triple next = iter.next();
        	if (mapInRelation.containsKey(next.getPredicate().toString())==false){
        		//System.out.println(next.getPredicate());
        		mapInRelation.put(next.getPredicate().toString(),r);
        		mapOutRelation.add(next.getPredicate().toString());
        		r++;
        	}
        }
        executor.shutdown();
        
        iter = new PipedRDFIterator<Triple>();
        final PipedRDFStream<Triple> inputStream2 = new PipedTriplesStream(iter);
        executor = Executors.newSingleThreadExecutor();
        
        parser = new Runnable() {
            @Override
            public void run() {
            	RDFDataMgr.parse(inputStream2, dump);
            }
        };
        executor.submit(parser);
        
		FileOutputStream matrixI = new FileOutputStream(System.getProperty("user.dir")+"/index/matrixI1");
		PrintStream printStreamI = new PrintStream(matrixI);
		FileOutputStream matrixR1 = new FileOutputStream(System.getProperty("user.dir")+"/index/matrixR1");
		PrintStream printStreamR1 = new PrintStream(matrixR1);
		FileOutputStream matrixR2 = new FileOutputStream(System.getProperty("user.dir")+"/index/matrixR2");
		PrintStream printStreamR2 = new PrintStream(matrixR2);
		int j=0;
		
		System.out.println("Parsing the dump ...");
		while ( iter.hasNext()){
			Triple next = iter.next();
			if (next.getObject().isURI()){
				Object s = mapIn.get(next.getSubject().toString());
				Object p = mapInRelation.get(next.getPredicate().toString()); 
				Object o = mapIn.get(next.getObject().toString());
				if (s!=null){
					if (s!=null && o!=null){
						printStreamI.print(s + " " + o + " 1\n");
						printStreamR1.print(s + " " + p + " 1\n");
						printStreamR2.print(p + " " + o + " 1\n");
						j++;
					} else {
						printStreamR1.print(s + " " + p + " 1\n");
					}	
				}	
			}
		}
		printStreamI.print(i + " " + i + " 0\n");
		printStreamI.close();
		printStreamR1.print(i + " " + r + " 0\n");
		printStreamR1.close();
		printStreamR2.print(r + " " + i + " 0\n");
		printStreamR2.close();
		executor.shutdown();
		
		System.out.println("Number triples: " + j);
		System.out.println("The shortest paths are computed ... ");
		//Use the octave instance to compute matrix multiplication
		
		
		
		//Compute the shortest path of length maximal 3
		octave.eval("load "+System.getProperty("user.dir")+"/index/matrixI1"+"; ");
		octave.eval("I1 = spconvert(matrixI1); ");
		
		octave.eval("I2=I1*I1;");
		octave.eval("I3=I2*I1;");
		
		octave.eval("B1=spones(I1);");
		octave.eval("B2=spones(I2);");
		octave.eval("B3=spones(I3);");
		
		octave.eval("clear I1;");
		octave.eval("clear I2;");
		octave.eval("clear I3;");
		
		octave.eval("C1=B1;");
		octave.eval("C2=B2-B1;");
		octave.eval("C3=B3-B2-B1;");
		
		octave.eval("clear B1;");
		octave.eval("clear B2;");
		octave.eval("clear B3;");
		
		octave.eval("D1=C1;");
		octave.eval("D2=spfun(@(x)x.*(x>=0),C2);");
		octave.eval("D3=spfun(@(x)x.*(x>=0),C3);");
		
		octave.eval("clear C1;");
		octave.eval("clear C2;");
		octave.eval("clear C3;");
		
		octave.eval("B=D1+2*D2+3*D3;");
		
		octave.eval("clear D1;");
		octave.eval("clear D2;");
		octave.eval("clear D3;");
		/*
		System.out.println("Transferring matrix from octave to java ... ");
		octave.eval("[i,j,val] = find(B);");
		octave.eval("data_dump = [i,j,val];");
		octave.eval("C = [i';j';val'];");
		
		octave.eval("fid = fopen(\'"+System.getProperty("user.dir")+"/index/export"+"\',\'w\');");
		octave.eval("fprintf( fid,\'%%%%MatrixMarket matrix coordinate real general\\n\' );");
		octave.eval("fprintf( fid,\'%d %d %d \\n\',size(B),nnz(B) );");
		octave.eval("fprintf( fid,\'%d %d %d\\n', C );");
		
		//Octave is not needed anymore
		octave.close();
		
		BufferedReader reader= new BufferedReader(new FileReader(System.getProperty("user.dir")+"/index/export"));
		MatrixVectorReader s = new MatrixVectorReader(reader);
		matrixIndex = new CompColMatrix(s);
		reader.close();
		*/
	}
}
