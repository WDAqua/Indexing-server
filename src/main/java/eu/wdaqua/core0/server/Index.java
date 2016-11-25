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
//Java wrapper around octave
import eu.wdaqua.core0.connection.Connection;
import eu.wdaqua.core0.graph.Digraph;
import eu.wdaqua.core0.graph.In;
//Sparse matrix implementation
import org.apache.commons.math3.linear.OpenMapRealMatrix;
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

import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.enums.TripleComponentRole;

public class Index {
	
	String[] dump = new String[1];
        HDTmap map ;

	private int rowI;
	private int rowR;
	private Digraph g;
        private HDTmap map1 = new HDTmap("/home_expes/dd77474h/dbpedia_2016/small.hdt");

	Index() throws IOException {
		//dump[0]="/home_expes/dd77474h/Indexing-server/reduced.hdt";
                //dump[0]="/home_expes/dd77474h/dbpedia_2016/dump.ttl";
		dump[0]="/home_expes/dd77474h/dbpedia_2016/small.ttl";
                map = new HDTmap("/home_expes/dd77474h/dbpedia_2016/small.hdt");
                
                //dump[0]="/home_expes/dd77474h/wikidata/wikidata.ttl";
                //String dump = "/home_expes/dd77474h/test_small.nt";
                //String dump = "/home_expes/dd77474h/wikidata/wikidata-instances-old.nt";
                //String dump = "/home_expes/dd77474h/wikidata/wikidata_change2.ttl";
                //String dump = "/home_expes/dd77474h/wikidata/out";
                //String dump = "/home_expes/dd77474h/dbpedia_2016/dump-en-P-CC.ttl";
                //String dump = "/home_expes/dd77474h/test_small.nt";
	}
	
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
			if (map.get(uris[v])!=-1){
				String s=uris[v].replace("http://dbpedia.org/ontology/","");
				if (Character.isUpperCase(s.charAt(0))==false && uris[v].contains("http://wdaqua/")==false){ //Check that it is not a class
				int n=map.get(uris[v]);
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
						}
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
					}
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
			return c;
	}

	public void index() throws IOException, ClassNotFoundException{
            org.apache.log4j.BasicConfigurator.configure();
/*
            PrintWriter writer = new PrintWriter("reduced_wikidata.nt", "UTF-8");
            writer.print("<http://wdaqua/dateLiteral> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://wdaqua/Date>  \n");
            writer.print("<http://wdaqua/literalNumber> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://wdaqua/Number>  \n");
            PipedRDFIterator<Triple> iter = parse(dump[0]);
                while ( iter.hasNext()){
                    Triple next = iter.next();
                    if (next.getPredicate().toString().contains("http://www.wikidata.org/prop/direct/")==true){
                    if (next.getObject().isURI()){
                        writer.print("<"+next.getSubject()+"> <"+next.getPredicate()+"> <"+next.getObject()+"> \n");
                    }
                    if (next.getObject().isLiteral()){
                        if (next.getObject().getLiteralDatatype()==XSDDatatype.XSDdate
                            || next.getObject().getLiteralDatatype()==XSDDatatype.XSDdateTime){
                            writer.print("<"+next.getSubject()+"> <"+next.getPredicate()+"> <http://wdaqua/dateLiteral> \n");               
                        }
                        if (next.getObject().getLiteralDatatype()==XSDDatatype.XSDdouble
                            || next.getObject().getLiteralDatatype()==XSDDatatype.XSDdecimal
                            || next.getObject().getLiteralDatatype()==XSDDatatype.XSDinteger){
                            writer.print("<"+next.getSubject()+"> <"+next.getPredicate()+"> <http://wdaqua/literalNumber> \n");
                        }
                    }
                    }
                }
                writer.close();
*/



                System.out.println("HDT loading ...");
                //HDTmap map1 = new HDTmap("/home_expes/dd77474h/Indexing-server/reduced.hdt");
                HDTmap map1 = new HDTmap("/home_expes/dd77474h/dbpedia_2016/small.hdt");
                //HDT hdt = HDTManager.loadHDT("../dbpedia_2016/dump-en-P-CC-yago.hdt", null);
                for (int i=0; i<13; i++){
                    System.out.println("Id "+i+" "+map1.get(i));
                    System.out.println("Id "+i+" "+map1.get(map1.get(i)));
                }
                System.out.println("http://dbpedia.org/resource/Barack_Obama"+map1.get("http://dbpedia.org/resource/Barack_Obama"));
                //Adddd some uri for literals
		Integer i=1;
		Integer before=0;
		int r=1;
                
                System.out.println(map1.nSubjects+map1.nObjects-map1.nShared);
		g = new Digraph(map1.nSubjects+map1.nObjects-map1.nShared);
		//g.addEdge(map.get("http://wdaqua/dateLiteral"), mapRelation.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), map.get("http://wdaqua/Date"));
		//g.addEdge(map.get("http://wdaqua/literalNumber"), mapRelation.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), map.get("http://wdaqua/Number"));
	
                int l=0;	
                for (int k=0; k< dump.length; k++) {
			final String d = dump[k];
			PipedRDFIterator<Triple> iter = parse(d);
			System.out.println("Parsing the dump "+d+" ...");
			while ( iter.hasNext()){
				Triple next = iter.next();
				if (k==0 || next.getPredicate().toString().contains("http://www.wikidata.org/prop/direct/")==true){
                                if (next.getObject().isURI()){
					Integer s = map1.get(next.getSubject().toString());
					Integer p = map1.get(next.getPredicate().toString());
					Integer o = map1.get(next.getObject().toString());
                                        if (s!=-1 && p!=-1 && o!=-1){
					    g.addEdge(s, p, o);
					}
				} else {
					Integer s = map1.get(next.getSubject().toString());
					Integer p = map1.get(next.getPredicate().toString());
					if (s!=-1 && p!=-1){
						g.addEdge(s, p);
						//Consider the case where the object is a literal
						//if (next.getObject().isLiteral()){
						//	if (next.getObject().getLiteralDatatype()==XSDDatatype.XSDdate
						//			|| next.getObject().getLiteralDatatype()==XSDDatatype.XSDdateTime){
						//		g.addEdge(s,p,map.get("http://wdaqua/dateLiteral"));
						//	}
						//	if (next.getObject().getLiteralDatatype()==XSDDatatype.XSDdouble
						//			|| next.getObject().getLiteralDatatype()==XSDDatatype.XSDdecimal
						//			|| next.getObject().getLiteralDatatype()==XSDDatatype.XSDinteger){
						//		g.addEdge(s,p,map.get("http://wdaqua/literalNumber"));
						//	}
						//}
					}
				}}
                                if (l%10000==0){
                                    System.out.println(l);
                                }
                                l++;
			}
		}
		rowI=i;
		rowR=r;

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
    
        public class HDTmap{
                private HDT hdt;
                public int nShared;
                public int nSubjects;
                public int nObjects;
                private int nPredicates;
                

                HDTmap(String location) throws IOException{
                    hdt = HDTManager.loadHDT(location, null);
                    nShared = (int)hdt.getDictionary().getNshared();
                    System.out.println("Shared "+nShared);
                    nSubjects = (int)hdt.getDictionary().getNsubjects();
                    System.out.println("Subjects "+nSubjects);
                    nPredicates = (int)hdt.getDictionary().getNpredicates();
                    System.out.println("Perdicates "+nPredicates);
                    nObjects = (int)hdt.getDictionary().getNobjects();
                    System.out.println("Objects "+nObjects);
                }

                int get(String resource){
                    int tmp = hdt.getDictionary().stringToId(resource, TripleComponentRole.SUBJECT);
                    if (tmp != -1){
                        return (tmp-1);
                    }
                    tmp = hdt.getDictionary().stringToId(resource, TripleComponentRole.OBJECT);
                    if (tmp != -1){
                        if (tmp<nShared){
                            return (tmp-1);
                        } else {
                            return (tmp-1)+nSubjects-nShared;
                        }
                    }
                    tmp = hdt.getDictionary().stringToId(resource, TripleComponentRole.PREDICATE);
                    if (tmp != -1){
                        return (tmp-1)+nSubjects+nObjects-nShared;
                    }
                    return -1;
                } 

                String get(int id){
                    if (id<nSubjects){
                        CharSequence tmp = hdt.getDictionary().idToString((id+1), TripleComponentRole.SUBJECT);
                        return tmp.toString();
                    }
                    if (id>=nSubjects && id <nSubjects+nObjects-nShared){
                        CharSequence tmp = hdt.getDictionary().idToString((id+1)-nSubjects+nShared, TripleComponentRole.OBJECT);
                        return tmp.toString();
                    }
                    if (id>=nSubjects+nObjects-nShared){
                        CharSequence tmp = hdt.getDictionary().idToString((id+1)-nSubjects-nObjects+nShared, TripleComponentRole.PREDICATE);
                        return tmp.toString();
                    }
                    return null;
                }
        }


}

