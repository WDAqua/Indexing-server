import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//import org.apache.jena.graph.Triple;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.ujmp.core.SparseMatrix;
import org.ujmp.core.bigintegermatrix.BigIntegerMatrix;
import org.ujmp.core.intmatrix.SparseIntMatrix;

import com.hp.hpl.jena.graph.Triple;

import dk.ange.octave.OctaveEngine;
import dk.ange.octave.OctaveEngineFactory;
import dk.ange.octave.type.OctaveDouble;
import dk.ange.octave.type.OctaveObject;
import no.uib.cipr.matrix.io.MatrixVectorReader;
import no.uib.cipr.matrix.sparse.CompColMatrix;
import no.uib.cipr.matrix.sparse.FlexCompColMatrix;

import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.SparseRealMatrix;



public class Index {
	private HashMap<String,Integer> mapIn = new HashMap<String,Integer>();
	//private TCharIntHashMap mapIn = new TCharIntHashMap();
	//private HTreeMap<String, Integer> mapInDisk = null;
	private ArrayList<String> mapOut = new ArrayList<String>();
	private HashMap<String,Integer> mapInRelation = new HashMap<String,Integer>();
	private ArrayList<String> mapOutRelation = new ArrayList<String>();
	boolean load=false;
	private String labels = "/Users/Dennis/Downloads/labels-en-uris_it.nt";
	//private String labels = "/Users/Dennis/Downloads/labels_en.nt";
	//private String dump = "/Users/Dennis/Downloads/infobox-properties-en-uris_it.nt";
	private String dump = "/Users/Dennis/Downloads/dump-it.nt";
	//private String dump = "/Users/Dennis/Downloads/mappingbased-properties-en-uris_it.nt";
	private OctaveEngine octave;
	private Integer sizeI;
	private CompColMatrix matrixIndex;
	
	
	private void load() throws IOException, ClassNotFoundException{
		InputStream file = new FileInputStream("/Users/Dennis/Downloads/mapIn");
	    InputStream buffer = new BufferedInputStream(file);
	    ObjectInput input = new ObjectInputStream (buffer);
	    mapIn = (HashMap<String,Integer>)input.readObject();
		buffer.close();
		file = new FileInputStream("/Users/Dennis/Downloads/mapOut");
	    buffer = new BufferedInputStream(file);
	    input = new ObjectInputStream (buffer);
	    mapOut = (ArrayList<String>)input.readObject();
		buffer.close();
		file = new FileInputStream("/Users/Dennis/Downloads/mapInRelation");
	    buffer = new BufferedInputStream(file);
	    input = new ObjectInputStream (buffer);
	    mapInRelation = (HashMap<String,Integer>)input.readObject();
	    file = new FileInputStream("/Users/Dennis/Downloads/mapOutRelation");
	    buffer = new BufferedInputStream(file);
	    input = new ObjectInputStream (buffer);
	    mapOutRelation = (ArrayList<String>)input.readObject();
		buffer.close();
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
				tmp[k] = ((Integer)(sizeI+mapInRelation.get(uri))).toString();
			} else {
				throw new IllegalArgumentException("The URI "+ uri +" is not in the index!");
			}
			k++;
		}
		String indeces = String.join(", ", tmp);
		long estimatedTime = System.currentTimeMillis() - startTime;
		System.out.println("Found: "+ estimatedTime);
		System.out.println(indeces);
		//Selects the submatrix containing the rows and columns contained in indeces
		startTime = System.currentTimeMillis();
		octave.eval("C = full(B(["+indeces+" ] , [ "+indeces+"]));");
		estimatedTime = System.currentTimeMillis() - startTime;
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
		if (load==false){
			load();
			load=true;
		}
		return (Integer) mapIn.get(URI);
	}
	
	
	public String get(Integer i) throws IOException, ClassNotFoundException{
		if (load==false){
			System.out.println("Begin loading");
			load();
			System.out.println("loaded");
			load=true;
		}
		return (String)mapOut.get(i);
	}
	
	public void index() throws IOException, ClassNotFoundException{	
		/*
		System.out.println("Hallo");
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
       */
        /*
        Analyzer analyzer = new StandardAnalyzer();
        Directory dir = new RAMDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		iwc.setOpenMode(OpenMode.CREATE);
		IndexWriter writer = new IndexWriter(dir, iwc);
        */
        /*
        TempDictionary dictionary = modHDT.getDictionary();
		//TempTriples triples = modHDT.getTriples();

        // Load RDF in the dictionary and generate triples
        dictionary.startProcessing();
        long num=0;
        long size=0;
        while(iter.hasNext()) {
        	TripleString triple = iter.next();
        	//triples.insert(
        			dictionary.insert(triple.getSubject(), TripleComponentRole.SUBJECT);
        			//dictionary.insert(triple.getPredicate(), TripleComponentRole.PREDICATE),
        			//dictionary.insert(triple.getObject(), TripleComponentRole.OBJECT)
        	//		);
        	num++;
			size+=triple.getSubject().length()+4;  // Spaces and final dot
        	ListenerUtil.notifyCond(listener, "Loaded "+num+" triples", num, 0, 100);
        }
        dictionary.endProcessing();
        */
        /*
        while (iter.hasNext()) {
            Triple next = iter.next();
            
            //Document doc = new Document();
            
            //doc.add(new StringField("id", i.toString(), Field.Store.YES));
            //doc.add(new TextField("URI", next.getSubject().toString(), Field.Store.NO));
            
            //mapInDisk.put(next.getSubject().toString(),i);
            
            mapIn.put(next.getSubject().toString(),i);
	    	mapOut.add(next.getSubject().toString());
	    	i++;
	    	if(i % 10000 == 0) {
	    		System.out.println(i);
	    		//db.commit();
	    	}
        }
        
        //writer.close();
        
        sizeI=i;
        System.out.println("Number"+i);
        executor.shutdownNow();
		
        //Store the mapIn, mapOut
        FileOutputStream f = new FileOutputStream("/Users/Dennis/Downloads/mapIn");  
        ObjectOutputStream stream = new ObjectOutputStream(f);          
        stream.writeObject(mapIn);
        stream.close();
        f = new FileOutputStream("/Users/Dennis/Downloads/mapOut");  
        stream = new ObjectOutputStream(f);          
        stream.writeObject(mapOut);
        stream.close();
        
        
        //RELATION EXTRACTION
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
        while ( iter.hasNext()){
        	Triple next = iter.next();
        	if (mapInRelation.containsKey(next.getPredicate().toString())==false){
        		//System.out.println(next.getPredicate());
        		mapInRelation.put(next.getPredicate().toString(),r);
        		mapOutRelation.add(next.getPredicate().toString());
        		r++;
        	}
        }
        System.out.println("r " + r);
        executor.shutdown();
        
      //Store the mapInRelation, mapOutRelation
        f = new FileOutputStream("/Users/Dennis/Downloads/mapInRelation");  
        stream = new ObjectOutputStream(f);          
        stream.writeObject(mapInRelation);
        stream.close();
        f = new FileOutputStream("/Users/Dennis/Downloads/mapOutRelation");  
        stream = new ObjectOutputStream(f);          
        stream.writeObject(mapOutRelation);
        stream.close();
		
        
        iter = new PipedRDFIterator<Triple>();
        final PipedRDFStream<Triple> inputStream2 = new PipedTriplesStream(iter);
        // PipedRDFStream and PipedRDFIterator need to be on different threads
        //ExecutorService executor2 = Executors.newSingleThreadExecutor();
        executor = Executors.newSingleThreadExecutor();
        
        parser = new Runnable() {
            @Override
            public void run() {
                // Call the parsing process.
                //RDFDataMgr.parse(inputStream2, "/Users/Dennis/Downloads/infobox-properties-en-uris_it.nt");
            	//RDFDataMgr.parse(inputStream2, "/Users/Dennis/Downloads/dump.nt");
            	RDFDataMgr.parse(inputStream2, dump);
            }
        };
		
        executor.submit(parser);
        
        //Matrix A = new LinkedSparseMatrix(i,i);
        
		FileOutputStream matrixI = new FileOutputStream("/Users/Dennis/Downloads/matrixI2");
		PrintStream printStream = new PrintStream(matrixI);
		FileOutputStream matrixR = new FileOutputStream("/Users/Dennis/Downloads/matrixR");
		PrintStream printStream2 = new PrintStream(matrixR);
		System.out.println("Hallo");
		int j=0;
		
		while ( iter.hasNext()){
			Triple next = iter.next();
			if (next.getObject().isURI()){
				Object s = mapIn.get(next.getSubject().toString());
				Object p = mapInRelation.get(next.getPredicate().toString()); 
				Object o = mapIn.get(next.getObject().toString());
				if (s!=null){
					if (s!=null && o!=null){
						printStream.print(s + " " + o + " 1\n");
						//printStream.print(o + " " + s + " 1\n");
						printStream2.print(s + " " + p + " 1\n");
						printStream2.print(o + " " + p + " 1\n");
						//A.add((int)s, (int)o, 1.0);
						j++;
					} else {
						printStream2.print(s + " " + p + " 1\n");
					}	
				}	
			}
		}
		printStream.print(i + " " + i + " 0\n");
		printStream.close();
		printStream2.print(i + " " + r + " 0\n");
		printStream2.close();
		executor.shutdown();
		
		System.out.println("j " + j);
		*/
		
		//Use the octave instance to compute matrix multiplication
		OctaveEngineFactory factory = new OctaveEngineFactory();
		factory.setOctaveProgram(new File("/usr/local/bin/octave"));
		octave = factory.getScriptEngine();
			
		//Compute the shortest path of length maximal 3
		octave.eval("load /Users/Dennis/Downloads/matrixI; ");
		octave.eval("I1 = spconvert(matrixI); ");
		
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
		
		octave.eval("[i,j,val] = find(B);");
		octave.eval("data_dump = [i,j,val];");
		
		octave.eval("fid = fopen(\'/Users/Dennis/Downloads/export\',\'w\');");
		octave.eval("fprintf( fid,\'%%%%MatrixMarket matrix coordinate real general\\n\' );");
		octave.eval("fprintf( fid,\'%d %d %d \\n\',size(B),nnz(B) );");
		octave.eval("fprintf( fid,\'%d %d %d\\n', data_dump );");
		
		//Octave is not needed anymore
		octave.close();
		
		
		
		//MatrixVectorReader r = new MatrixVectorReader(new BufferedReader(new FileReader("/Users/Dennis/Downloads/matrixExport")));
		
		//CompColMatrix test = new CompColMatrix(r);
		
		//int numColumns = 793452;
		//int[][] nz = new int[numColumns][0];
	        
	    
		//SparseIntMatrix m = (SparseIntMatrix) SparseMatrix(1,2); .Factory.zeros(793452, 793452);
		//SparseMatrix sparse = SparseMatrix.Factory.zeros(793452, 793452);
		
		/*
		Scanner s = new Scanner(new BufferedReader(new FileReader("/Users/Dennis/Downloads/matrixExport")));
        int i=0;
        int j=0;
        
        int[] row = new int[26219740];
        int[] column = new int[26219740];
        double[] entry = new double[26219740];
        
        List col = new ArrayList();
		while (s.hasNext()) {
			int a = Double.valueOf(s.next()).intValue()-1;
	       	int b = Double.valueOf(s.next()).intValue()-1;
	       	int c = Double.valueOf(s.next()).intValue();
	       	
	       	row[i]=a;
	       	column[i]=b;
	       	entry[i]=c;
	       	
	       	if (j==b){
        		col.add(a);
        		//System.out.println(a);
        	 } else {
	        	 nz[j]=new int [col.size()];
	        	 for (int k=0; k<col.size(); k++){
	        		 nz[j][k]=(int) col.get(k);
	        	 }
	        	 j++;
	        	 col.clear();
	        	 col.add(a);
	        	 //m.setAsInt(c, a, b);
        	 }
        	 i++;
        	 if (i%10000 == 0){
        		 System.out.println(i);
        	 }
         } 
		nz[numColumns-1]=new int [col.size()];
   	 	for (int k=0; k<col.size(); k++){
   	 		nz[numColumns-1][k]=(int) col.get(k);
   	 	}
		/*
		int[][] nz = new int[4][];
		nz[0] = new int [1];
		nz[1] = new int [0];
		nz[2] = new int [0];
		nz[3] = new int [1];
		nz[0][0] = 2;
		nz[3][0] = 1;
		*/
		
		System.out.println("Start");
		BufferedReader r= new BufferedReader(new FileReader("/Users/Dennis/Downloads/export"));
		MatrixVectorReader s = new MatrixVectorReader(r);
		matrixIndex = new CompColMatrix(s);
		System.out.println("End");
		
		int[] row = new int[200];
		//int[] col = new int[200];
		for (int i=0; i< 200 ; i++){
				row[i]=(int)(Math.random() * 793452);
		}
		
		long startTime = System.currentTimeMillis();
		for (int i=0; i< 200 ; i++){
			for (int j=0; j< 200 ; j++){
				matrixIndex.get(i, j);
			}
		}
		long estimatedTime = System.currentTimeMillis() - startTime;
		
		System.out.println(estimatedTime);
		//System.out.println(test.get(21682, 0));
		
		//ObjectSizeFetcher.getObjectSize(test);
		
		//for (int k=0; k<26219740; k++){
		//	test.set(row[k], column[k], entry[k]);
		//}
		//CompColMatrix test = new CompColMatrix(test);
		
	        
		
		/*
		OpenMapRealMatrix B = new  OpenMapRealMatrix(793452,793452); 
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
		*/
		
		/*
		// To include also the relations
		octave.eval("load /Users/Dennis/Downloads/matrixR; ");
		octave.eval("R = spconvert(matrixR); ");
		octave.eval("IR = I*R; ");
		octave.eval("IRT = IR'; ");
		octave.eval("IRTR = IRT*R; ");
		octave.eval("A1=[I IR];");
		octave.eval("A2=[IRT, IRTR];");
		octave.eval("A=[A1 ; A2];");
		octave.eval("clear IR;");
		octave.eval("clear IRT;");
		octave.eval("clear IRTR;");
		octave.eval("clear A1;");
		octave.eval("clear A2;");
		
		octave.eval("I2 = I*I; ");
		octave.eval("I2R = I2*R; ");
		octave.eval("I2RT = I2R'; ");
		octave.eval("I2RTR = I2RT*R; ");
		octave.eval("B1=[I2 I2R];");
		octave.eval("B2=[I2RT, I2RTR];");
		octave.eval("B=[B1 ; B2];");
		
		octave.eval("clear I2;");
		octave.eval("clear I2R;");
		octave.eval("clear I2RT;");
		octave.eval("clear B1;");
		octave.eval("clear B2;");
		*/
		load();
		load=true;
	}
}
