import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
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
import org.apache.jena.ext.com.google.common.collect.BiMap;
import org.apache.jena.ext.com.google.common.collect.HashBiMap;
import org.apache.jena.graph.Triple;
//For parsing RDF files
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.log4j.BasicConfigurator;
//import org.jgrapht.DirectedGraph;
//import org.jgrapht.graph.DefaultDirectedGraph;
//import org.jgrapht.graph.DefaultEdge;

import algorithms.Digraph;
import algorithms.In;

public class Index {
	
	String dump = "/Users/Dennis/Downloads/mappingbased_objects_en_uris_it.ttl";
	//String dump = "/home_expes/dd77474h/test_small.nt";
	
	private BiMap<String, Integer> map = HashBiMap.create();
	// ArrayList<String> mapOut = new ArrayList<String>();
	private BiMap<String, Integer> mapRelation = HashBiMap.create();
	//private HashMap<String,Integer> mapRelation = new HashMap<String,Integer>();
	//private ArrayList<String> mapOutRelation = new ArrayList<String>();
	private CompColMatrix matrixIndex;
	private OctaveEngine octave;
	private int rowI;
	private int rowR;
	
	private Digraph g;
	
	public OpenMapRealMatrix get(String[] uris) throws IllegalArgumentException, FileNotFoundException{
		HashMap<String,Integer> urisHash = new HashMap<String, Integer>();
		for (int l=0; l < uris.length; l++){
			urisHash.put(uris[l],l);
		}
		
		long startTime = System.currentTimeMillis();
		OpenMapRealMatrix B = new  OpenMapRealMatrix(uris.length,uris.length);
		int i=0;
        ArrayList<String> results=new ArrayList<String>();
        //Random rand = new Random();
        for (int v = 0; v < uris.length; v++) {
        	if (map.containsKey(uris[v])){
	        	System.out.println(v);
	        	int n=map.get(uris[v]);
	        		results.add(map.inverse().get(n));
	        		//System.out.println(v);
	                for (int w=0; w < g.adj_out(n).size(); w++) {
	                	String edge=mapRelation.inverse().get(g.edge_out(n).get(w));
	                	String node=map.inverse().get(g.adj_out(n).get(w));
	                	if (urisHash.containsKey(edge)){
	                		B.setEntry(v, urisHash.get(edge), 1.0);
	                	}
	                	if (urisHash.containsKey(node)){
	                		B.setEntry(v, urisHash.get(node), 2.0);
	                	}
	                	results.add("   "+edge+" --- "+node);
	                    for (int l=0; l<g.adj_out(w).size(); l++){
	                    	edge=mapRelation.inverse().get(g.edge_out(w).get(l));
	                    	node=map.inverse().get(g.adj_out(w).get(l));
	                    	i++;
	                    	if (urisHash.containsKey(edge) && B.getEntry(v, urisHash.get(edge))==0){
		                		B.setEntry(v, urisHash.get(edge), 3.0);
		                	}
		                	if (urisHash.containsKey(node) && B.getEntry(v, urisHash.get(node))==0){
		                		//System.out.println("Here"+B.getEntry(v, urisHash.get(mapOut.get(g.adj_out(w).get(l)))));
		                		B.setEntry(v, urisHash.get(node), 4.0);
		                	}
	                    	//System.out.println("    "+mapOut.get(l));
	                    	results.add("      "+edge+" --- "+node);
	                    	//big.addEdge(v, l);
	                    }
	                    /*
	                    //Go back
	                    for (int l=0; l<g.adj_in(w).size(); l++){
	                    	i++;
	                    	if (urisHash.containsKey(mapOutRelation.get(g.edge_in(w).get(l)))){
		                		B.setEntry(v, urisHash.get(mapOutRelation.get(g.edge_in(w).get(l))), -3.0);
		                	}
		                	if (urisHash.containsKey(mapOut.get(g.adj_in(w).get(l)))){
		                		B.setEntry(v, urisHash.get(mapOut.get(g.adj_in(w).get(l))), -4.0);
		                	}
	                    	//System.out.println("    "+mapOut.get(l));
	                    	//results.add("<     "+mapOutRelation.get(g.edge_in(w).get(l))+" --- "+mapOut.get(g.adj_in(w).get(l)));
	                    	//big.addEdge(v, l);
	                    }*/
	                }
	                for (int w=0; w<g.adj_in(n).size(); w++){
	                	String edge=mapRelation.inverse().get(g.edge_in(n).get(w));
	                	String node=map.inverse().get(g.adj_in(n).get(w));
	                	if (urisHash.containsKey(edge)){
	                		B.setEntry(urisHash.get(edge), v , 1.0);
	                	}
	                	if (urisHash.containsKey(node)){
	                		B.setEntry(urisHash.get(node), v, 2.0);
	                	}
	                	i++;
                    	results.add("<  "+edge+" --- "+node);
                    	for (int l=0; l<g.adj_out(w).size(); l++){
	                    	edge=mapRelation.inverse().get(g.edge_out(w).get(l));
	                    	node=map.inverse().get(g.adj_out(w).get(l));
                    		i++;
	                    	if (urisHash.containsKey(edge) && B.getEntry(v, urisHash.get(edge))==0){
		                		B.setEntry(v, urisHash.get(edge), -3.0);
		                	}
	                    	if (urisHash.containsKey(node) && (int)B.getEntry(v, urisHash.get(node))==0){
		                		B.setEntry(v, urisHash.get(node), -4.0);
		                	}
	                    	//System.out.println("    "+mapOut.get(l));
	                    	results.add("<<    "+edge+" --- "+node);
	                    	//big.addEdge(v, l);
	                    }
                    }
	        	}
	        }
	        long estimatedTime = System.currentTimeMillis() - startTime;
	        for (String s: results){
	        	System.out.println(s);
	        }
	        System.out.println("Time"+estimatedTime);
	        System.out.println("Traversed edges"+i);
	        
	        for (int m=0; m<uris.length; m++){
                for (int n=0; n<uris.length; n++){
                                System.out.print(B.getEntry(m, n)+", ");
                }
                System.out.println("");
	        }
        return B;
	}
	
	
	public Integer get(String URI) throws IOException, ClassNotFoundException{
		return (Integer) map.get(URI);
	}
	
	
	public String get(Integer i) throws IOException, ClassNotFoundException{
		return (String)map.inverse().get(i);
	}
	
	public void index() throws IOException, ClassNotFoundException{
		org.apache.log4j.Logger.getRootLogger().setLevel(org.apache.log4j.Level.OFF);
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
                RDFDataMgr.parse(inputStream, dump);
            }
        };

        // Start the parser on another thread
        executor.submit(parser);
        Integer i=1;
        Integer before=0;
        int r=1;
        //mapOut.add("null");
        //mapOutRelation.add("add");
        System.out.println("Parse the dump and search for all subjects objects and relations ...");
        System.out.println("If this slows down, and does not come to an end, you probably need more RAM!");
        while (iter.hasNext()) {
            Triple next = iter.next();
            // Look if subject already encounterd
            if (map.containsKey(next.getSubject().toString())==false){
            	map.put(next.getSubject().toString(),i);
    	    	//mapOut.add(next.getSubject().toString());
    	    	i++;
        		
        	}
            //Look if the relation was already encountered
            if (mapRelation.containsKey(next.getPredicate().toString())==false){
        		//System.out.println(next.getPredicate());
        		mapRelation.put(next.getPredicate().toString(),r);
        		//mapOutRelation.add(next.getPredicate().toString());
        		r++;
        	}
            //Look if object is an uri and was already encountered
            if (next.getObject().isURI()==true){ 
            	if (map.containsKey(next.getObject().toString())==false){
	            	map.put(next.getObject().toString(),i);
	    	    	//mapOut.add(next.getObject().toString());
	    	    	i++;
            	}
            }
	    	if(i % 1000000 == 0) {
	    		if (i!=before){
	 	    		System.out.println(i);
	 	    	}
	    		before=i;
	    	}
        }
        System.out.println("Number resources: "+i);
        System.out.println("Number relations: "+r);
        executor.shutdownNow();
        
        
        g = new Digraph(i);
        
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
        
        
		
		int j=0;
		System.out.println("Parsing the dump ...");
		while ( iter.hasNext()){
			Triple next = iter.next();
			if (next.getObject().isURI()){
				Integer s = map.get(next.getSubject().toString());
				Integer p = mapRelation.get(next.getPredicate().toString()); 
				Integer o = map.get(next.getObject().toString());
				if (s!=null){
					if (s!=null && o!=null){
						g.addEdge(s, p, o);
						j++;
					} else {
						j++;
					}	
				}	
			} else {
				Object s = map.get(next.getSubject().toString());
				Object p = mapRelation.get(next.getPredicate().toString());
				if (s!=null){
					//printStreamR1.print(s + " " + p + " 1\n");
				}
			}
		}
		executor.shutdown();
		
		rowI=i;
		rowR=r;
     
	}
}
