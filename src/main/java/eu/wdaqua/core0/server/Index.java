package eu.wdaqua.core0.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Iterator;
import java.lang.Iterable;
//Java wrapper around octave
import eu.wdaqua.core0.connection.Connection;
import eu.wdaqua.core0.graph.Digraph;
import eu.wdaqua.core0.graph.In;
//Sparse matrix implementation
import org.apache.jena.datatypes.xsd.XSDDatatype;
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

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import static java.lang.Integer.min;

import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;

import org.neo4j.io.fs.FileUtils;
import eu.wdaqua.core0.server.DepthFirstBounded;

public class Index {

        String[] dump = {"/home_expes/dd77474h/wikidata/wikidata.ttl"};
        //String[] dump = {"/home_expes/dd77474h/dbpedia_2016/dump.ttl"};

	private static final File DB_PATH = new File( "/home/dd77474h/neo4j-community-3.0.7/data/databases/graph.db" );
	private static final String DB_CONFIG_PATH = "/home/dd77474h/neo4j-community-3.0.7/conf/neo4j.conf";	
	
	private int rowI;
	private int rowR;
        private GraphDatabaseService graphDb;

        public enum Labels implements Label {
            Resource,
        }

	Index() throws IOException, ClassNotFoundException {
	    index();
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( DB_PATH ).loadPropertiesFromFile( DB_CONFIG_PATH ).newGraphDatabase();
            //Query for "Warm the cache"
            String query= "MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN count(n.uri)+count(r.uri);";
            //String query="CALL apoc.warmup.run()";
            graphDb.execute(query);
            /* 
            try ( Transaction tx = graphDb.beginTx()) {
                for ( Node n : GlobalGraphOperations.at(graphDb).getAllNodes()) {
                    n.getPropertyKeys();
                    for ( Relationship relationship : n.getRelationships()) {
                        relationship.getPropertyKeys();
                        relationship.getStartNode();
                    }
                }
            }*/
            registerShutdownHook( graphDb );
            //String[] uris2 = {"http://www.wikidata.org/entity/Q5"};
            //this.get(uris2);
        }
	
	public Connection get(String[] uris) throws IllegalArgumentException, FileNotFoundException{
	    try ( Transaction tx = graphDb.beginTx() ){
                HashMap<String,Integer> urisHash = new HashMap<String, Integer>();
		for (int l=0; l < uris.length; l++){
			urisHash.put(uris[l],l);
		}

                //String[] uris2 = {"http://dbpedia.org/resource/Buddenbrooks", "http://dbpedia.org/resource/Thomas_Mann"};
                //uris = uris2;

		long startTime = System.currentTimeMillis();
		Connection c = new Connection();
		int traversedEdges=0;
		HashSet<String> relations = new HashSet<String>();
		for (int v = 0; v < uris.length; v++) {
                    Node n = graphDb.findNode(Labels.Resource, "uri", uris[v]);
                    if (n!=null && Character.isUpperCase(uris[v].replace("http://dbpedia.org/ontology/","").charAt(0))==false && uris[v].contains("http://wdaqua/")==false){
				//if (Character.isUpperCase(s.charAt(0))==false && uris[v].contains("http://wdaqua/")==false){ //Check that it is not a class
                        System.out.println("Uri "+uris[v]);
                        Evaluator evaluator_out = new Evaluator() {
                            public Evaluation evaluate(Path path) {
                                if(path.length() == 0){
                                    return Evaluation.EXCLUDE_AND_CONTINUE;
                                } else if (path.length() == 1){
                                        return Evaluation.INCLUDE_AND_CONTINUE;
                                } else {
                                    return Evaluation.INCLUDE_AND_PRUNE;
                                }
                            }
                        };
                        //Search forward
                        long startTime1 = System.currentTimeMillis();
                        TraversalDescription td = graphDb.traversalDescription()
                                                .depthFirst()
                                                /*.order(
            						new BranchOrderingPolicy(){
                						public BranchSelector create( TraversalBranch startSource, PathExpander pathExpander ){
                    							return new DepthFirstBounded( startSource, 5, pathExpander );
                						}
            					} )*/
						.expand(new ForwardDirection())
                                                .evaluator(evaluator_out)
						.uniqueness(Uniqueness.NODE_PATH);
                        int count = 0;
                        int forward=0;
                        for (Path path: td.traverse(n)) {
                            //System.out.println(path);
			    forward++;
                            traversedEdges++;
                            count++;
                            Iterator<Node> it = path.nodes().iterator();
                            Iterator<Relationship> it_rel = path.relationships().iterator();
                            it.next();
                            if (it.hasNext()){
                                Relationship relationship = it_rel.next();
                                Node node = it.next();
                                boolean edge1_found=false;
                                String edge1=relationship.getType().toString();
                                if (urisHash.containsKey(edge1)){
                                    c.add(v, urisHash.get(edge1), 1);
                                    edge1_found=true;
                                }
                                String node1 = node.getProperty("uri").toString();
                                //System.out.println(uri);
                                if (urisHash.containsKey(node1)){
                                    c.add(v, urisHash.get(node1), 2);
                                }
                                if (it.hasNext()){
                                    relationship = it_rel.next();
                                    node = it.next();
                                    String edge2=relationship.getType().toString();
                                    if (urisHash.containsKey(edge2)){
                                        c.add(v, urisHash.get(edge2), 3);
                                        if (edge1_found==true){
                                            c.add(urisHash.get(edge1), urisHash.get(edge2), 2);
                                        }
                                    }
                                    String node2 = node.getProperty("uri").toString();                    
                                    if (urisHash.containsKey(node2)){
                                        c.add(v, urisHash.get(node2), 4);
                                        if (edge1_found==true){
                                            c.add(urisHash.get(edge1), urisHash.get(node2), 3);
                                        }
                                    }
                                }
                            }	
                        }
                        long estimatedTime1 = System.currentTimeMillis() - startTime1;
                        System.out.println("Time "+estimatedTime1);
                        System.out.println("Traversed forward "+forward);
                        //Search backwards-forwards
                        long startTime2 = System.currentTimeMillis();
                        Evaluator evaluator_in = new Evaluator() {
                            public Evaluation evaluate(Path path) {
                                if(path.length() == 0){
                                    //if (path.endNode().getDegree(Direction.INCOMING)<100000){
                                        return Evaluation.EXCLUDE_AND_CONTINUE;
                                    //} else {
                                        //System.out.println("PRUNED "+path.endNode().getDegree(Direction.INCOMING));
                                        //return Evaluation.EXCLUDE_AND_PRUNE;
                                    //}
                                } else if (path.length() == 1){
                                    String uri = path.endNode().getProperty("uri").toString();
                                    //String uri = path.endNode().toString();
                                    if ( Character.isUpperCase(uri.replace("http://dbpedia.org/ontology/","").charAt(0))
                                        || uri.contains("http://wdaqua/") ){
                                        return Evaluation.INCLUDE_AND_PRUNE;
                                    } else {
                                        return Evaluation.INCLUDE_AND_CONTINUE;
                                    }
                                } else {
                                    return Evaluation.INCLUDE_AND_PRUNE;
                                }
                            }
                        };
                        td = graphDb.traversalDescription()
                                                //.breadthFirst()
                                                .order(
                                                        new BranchOrderingPolicy(){
                                                                public BranchSelector create( TraversalBranch startSource, PathExpander pathExpander ){
                                                                        return new DepthFirstBounded( startSource, 100000, pathExpander );
                                                                }
                                                } )
						.expand(new BackwardDirection())
                                                .evaluator(evaluator_in)
						.uniqueness(Uniqueness.NODE_PATH);
                        int backwards=0;
                        for (Path path: td.traverse(n)) {
                            traversedEdges++;
                            backwards++;
                            count++;
                            //System.out.println(path);
                            Iterator<Node> it = path.nodes().iterator();
                            Iterator<Relationship> it_rel = path.relationships().iterator();
                            it.next();
                            if (it.hasNext()){
                                Relationship relationship = it_rel.next();
                                Node node = it.next();
                                boolean edge1_found=false;
                                String edge1=relationship.getType().toString();
                                if (urisHash.containsKey(edge1)){
                                    c.add(v, urisHash.get(edge1), -1);
                                    edge1_found=true;
                                }
                                String node1 = node.getProperty("uri").toString();
                                //System.out.println(uri);
                                if (urisHash.containsKey(node1)){
                                    c.add(v, urisHash.get(node1), -2);
                                }
                                if (it.hasNext()){
                                    relationship = it_rel.next();
                                    node = it.next();
                                    String edge2=relationship.getType().toString();
                                    if (urisHash.containsKey(edge2)){
                                        c.add(v, urisHash.get(edge2), -3);
                                        if (edge1_found==true){
                                            c.add(urisHash.get(edge1), urisHash.get(edge2), -2);
                                        }
                                    }
                                    String node2 = node.getProperty("uri").toString();
                                    if (urisHash.containsKey(node2)){
                                        c.add(v, urisHash.get(node2), -4);
                                        if (edge1_found==true){
                                            c.add(urisHash.get(edge1), urisHash.get(node2), -3);
                                        }
                                    }
                                }
                            }
                        }
                        long estimatedTime2 = System.currentTimeMillis() - startTime2;
                        System.out.println("Time backwards "+estimatedTime2);
                        System.out.println("Backwards "+backwards);
                    }
                }
                long estimatedTime = System.currentTimeMillis() - startTime;
                System.out.println("Time total "+estimatedTime);
                System.out.println("Traversed edges "+traversedEdges);
                System.out.println("Number relations "+relations.size()); 
                return c;
            }
        }            
/*
					for (int w=0; w < g.adj_out(n).size(); w++) {
						int next = g.adj_out(n).get(w);
						String edge1=map.get(g.edge_out(n).get(w));
						boolean edge1_found=false;
								relations.add(edge1);
						String node1=map.get(g.adj_out(n).get(w));
						if (urisHash.containsKey(edge1)){
							c.add(v, urisHash.get(edge1), 1);
							edge1_found=true;
						}
						if (urisHash.containsKey(node1)){
							c.add(v, urisHash.get(node1), 2);
						}
						for (int l=0; l<g.adj_out(next).size(); l++){
								String edge2=map.get(g.edge_out(next).get(l));
								String node2=map.get(g.adj_out(next).get(l));
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
						}*/
						/*
						//Go back but not for classes
						if (node1!=null){
						s=node1.replace("http://dbpedia.org/ontology/","");
								if (Character.isUpperCase(s.charAt(0))==false){
									for (int l=0; l<g.adj_in(w).size(); l++){
								String edge=map.get(g.edge_out(n).get(w));
								String node=map.get(g.adj_out(n).get(w));
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
				/*	}
					//limits the search to 5000 ingoing edges, to huge for classes
					for (int w=0; w<min(g.adj_in(n).size(),5000); w++){
						int next = g.adj_in(n).get(w);
						String edge1=map.get(g.edge_in(n).get(w));
						boolean edge1_found=false;
						relations.add(edge1);
						String node1=map.get(g.adj_in(n).get(w));
						if (urisHash.containsKey(edge1)){
							c.add(v, urisHash.get(edge1), -1);
							edge1_found=true;
						}
						if (urisHash.containsKey(node1)){
							c.add(v, urisHash.get(node1), -2);
						}
						i++;
						for (int l=0; l<g.adj_out(next).size(); l++){
							String edge2=map.get(g.edge_out(next).get(l));
							String node2=map.get(g.adj_out(next).get(l));
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
						}
					}
				}
			}
			}
			long estimatedTime = System.currentTimeMillis() - startTime;
			System.out.println("Time "+estimatedTime);
			System.out.println("Traversed edges "+i);
			System.out.println("Number relations "+relations.size());
*/

	public void index() throws IOException, ClassNotFoundException{
            org.apache.log4j.BasicConfigurator.configure();

            PrintWriter writer = new PrintWriter("reduced_wikidata.ttl", "UTF-8");
            writer.print("<http://wdaqua/literalDate> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://wdaqua/Date> . \n");
            writer.print("<http://wdaqua/literalNumber> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://wdaqua/Number> . \n");
            writer.print("<http://wdaqua/literalLiteral> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://wdaqua/Literal> . \n");
            PipedRDFIterator<Triple> iter = parse(dump[0]);
                while ( iter.hasNext()){
                    Triple next = iter.next();
                    if (next.getPredicate().toString().contains("http://www.wikidata.org/prop/direct/")==true){
                    if (next.getObject().isURI()){
                        writer.print("<"+next.getSubject()+"> <"+next.getPredicate()+"> <"+next.getObject()+"> . \n");
                    }
                    if (next.getObject().isLiteral()){
                        if (next.getObject().getLiteralDatatype()==XSDDatatype.XSDdate
                            || next.getObject().getLiteralDatatype()==XSDDatatype.XSDdateTime){
                            writer.print("<"+next.getSubject()+"> <"+next.getPredicate()+"> <http://wdaqua/literalDate> . \n");               
                        } else if (next.getObject().getLiteralDatatype()==XSDDatatype.XSDdouble
                            || next.getObject().getLiteralDatatype()==XSDDatatype.XSDdecimal
                            || next.getObject().getLiteralDatatype()==XSDDatatype.XSDinteger
                            || next.getObject().getLiteralDatatype()==XSDDatatype.XSDnonNegativeInteger){
                            writer.print("<"+next.getSubject()+"> <"+next.getPredicate()+"> <http://wdaqua/literalNumber> . \n");
                        } else {
                            writer.print("<"+next.getSubject()+"> <"+next.getPredicate()+"> <http://wdaqua/literalLiteral> . \n");
                        }
                    }
                    }
                }
                writer.close();
        }
    

	public static PipedRDFIterator<Triple> parse(final String dump){
		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>(100000, false , 30000, 100);
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
		return iter;
	}

        private static void registerShutdownHook( final GraphDatabaseService graphDb ){
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }


    private class ForwardDirection<STATE> implements PathExpander<STATE>{
        @Override
        public Iterable<Relationship> expand(Path path, BranchState<STATE> state) {
            if (path.length()==0) {
                return path.endNode().getRelationships(Direction.OUTGOING);
            } else {
                return path.endNode().getRelationships(Direction.OUTGOING);
            }
        }

        @Override
        public PathExpander<STATE> reverse() {
            return this;
        }
    }

    private class BackwardDirection<STATE> implements PathExpander<STATE>{
        @Override
        public Iterable<Relationship> expand(Path path, BranchState<STATE> state) {
            if (path.length()==0) {
                return path.endNode().getRelationships(Direction.INCOMING);
            } else {
                return path.endNode().getRelationships(Direction.OUTGOING);
            }
        }

        @Override
        public PathExpander<STATE> reverse() {
            return this;
        }
    }
}

