package eu.wdaqua.core0.Server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//Java wrapper around octave
import eu.wdaqua.core0.connection.Connection;
import eu.wdaqua.core0.graph.Digraph;
import eu.wdaqua.core0.graph.In;
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

import com.google.common.collect.Table;
import com.google.common.collect.HashBasedTable;

public class Index {
	
	String dump = "/home_expes/dd77474h/dbpedia_2016/dump-en-P-CC.ttl";
	//String dump = "/home_expes/dd77474h/test_small.nt";
	
	private BiMap<String, Integer> map = HashBiMap.create();
	private BiMap<String, Integer> mapRelation = HashBiMap.create();
	private CompColMatrix matrixIndex;
	private int rowI;
	private int rowR;
	
	private Digraph g;
	
	public Connection get(String[] uris) throws IllegalArgumentException, FileNotFoundException{
		HashMap<String,Integer> urisHash = new HashMap<String, Integer>();
		for (int l=0; l < uris.length; l++){
			urisHash.put(uris[l],l);
		}
		
		long startTime = System.currentTimeMillis();
		Connection c = new Connection();
		int i=0;
        ArrayList<String> results=new ArrayList<String>();
        HashSet<String> relations = new HashSet<String>();
	for (int v = 0; v < uris.length; v++) {
        	if (map.containsKey(uris[v])){
		String s=uris[v].replace("http://dbpedia.org/ontology/","");
                if (Character.isUpperCase(s.charAt(0))==false){
	        	int n=map.get(uris[v]);
	                for (int w=0; w < g.adj_out(n).size(); w++) {
	                	int next = g.adj_out(n).get(w);
				String edge1=mapRelation.inverse().get(g.edge_out(n).get(w));
				boolean edge1_found=false;
	                	relations.add(edge1);
				String node1=map.inverse().get(g.adj_out(n).get(w));
				boolean node1_found=false;
	                	if (urisHash.containsKey(edge1)){
	                		c.add(v, urisHash.get(edge1), 1);
					edge1_found=true;
	                	}
	                	if (urisHash.containsKey(node1)){
	                		c.add(v, urisHash.get(node1), 2);
					node1_found=true;
	                	}
	                	for (int l=0; l<g.adj_out(next).size(); l++){
	                    		String edge2=mapRelation.inverse().get(g.edge_out(next).get(l));
	                    		String node2=map.inverse().get(g.adj_out(next).get(l));
	                    		i++;
	                    		if (urisHash.containsKey(edge2)){
						c.add(v, urisHash.get(edge2), 3);
						if (edge1_found==true){
							c.add(urisHash.get(edge1), urisHash.get(edge2), 2);
						}
		                	}
		                	if (urisHash.containsKey(node2)){
						c.add(v, urisHash.get(node2), 4);
						if (edge1_found==true){
							c.add(urisHash.get(edge1), urisHash.get(node2), 3);
                                                }
		                	}
	                    	}
				/*
				//Go back but not for classes
				if (node1!=null){
				s=node1.replace("http://dbpedia.org/ontology/","");
                		if (Character.isUpperCase(s.charAt(0))==false){
	                   		for (int l=0; l<g.adj_in(w).size(); l++){
						String edge=mapRelation.inverse().get(g.edge_out(n).get(w));
						String node=map.inverse().get(g.adj_out(n).get(w));
						i++;
	                    			if (urisHash.containsKey(edge)){
		                			B.setEntry(v, urisHash.get(edge), -3.0);
		                		}
		                		if (urisHash.containsKey(node)){
		                			B.setEntry(v, urisHash.get(node), -4.0);
		                		}
	                    		}
				}
				}*/
	                }
	                for (int w=0; w<g.adj_in(n).size(); w++){
	                	int next = g.adj_in(n).get(w);
				String edge1=mapRelation.inverse().get(g.edge_in(n).get(w));
	                	boolean edge1_found=false;
				relations.add(edge1);
				String node1=map.inverse().get(g.adj_in(n).get(w));
	                	if (urisHash.containsKey(edge1)){
					c.add(v, urisHash.get(edge1), -1);
					edge1_found=true;
	                	}
	                	if (urisHash.containsKey(node1)){
	                		c.add(v, urisHash.get(node1), -2);
	                	}
	                	i++;
                    		//results.add("<  "+edge1+" --- "+node1);
                    		for (int l=0; l<g.adj_out(next).size(); l++){
	                    		String edge2=mapRelation.inverse().get(g.edge_out(next).get(l));
	                    		String node2=map.inverse().get(g.adj_out(next).get(l));
                    			i++;
	                    		if (urisHash.containsKey(edge2)){
		                		c.add(v, urisHash.get(edge2), -3);
						if (edge1_found==true){
							c.add(urisHash.get(edge1), urisHash.get(edge2), -2);
						}
		                	}
	                    		if (urisHash.containsKey(node2)){
						c.add(v, urisHash.get(node2), -4);
						if (edge1_found==true){
							c.add(urisHash.get(edge1), urisHash.get(node2), -3);
                                                }
		                	}
	                    		//results.add("<<    "+edge2+" --- "+node2);
	                	}
                    	}
	        	}
			}
	        }
	        long estimatedTime = System.currentTimeMillis() - startTime;
	        for (String s: results){
	        	System.out.println(s);
	        }
	        System.out.println("Time "+estimatedTime);
	        System.out.println("Traversed edges "+i);
	        System.out.println("Number relations "+relations.size());
        return c;
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
				Integer s = map.get(next.getSubject().toString());
				Integer p = mapRelation.get(next.getPredicate().toString());
				if (s!=null){
					g.addEdge(s, p);
					//printStreamR1.print(s + " " + p + " 1\n");
				}
			}
		}
		executor.shutdown();
		
		rowI=i;
		rowR=r;
 		System.out.println("Number triples "+j);    
	}
}


