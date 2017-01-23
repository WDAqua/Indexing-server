package eu.wdaqua.core0.server;

import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;

/**
 * A preorder depth first selector which detects "super nodes", i.e. nodes
 * which has many relationships and travers them only partially
 * 
 * @author Dennis Diefenbach
 */

public class DepthFirstBounded implements BranchSelector
{
    private TraversalBranch current;
    private final int threshold;
    private final PathExpander expander;
    private int number = 1;
	 
    public DepthFirstBounded( TraversalBranch startSource, int startThreshold, PathExpander expander )
    {
        this.current = startSource;
        this.threshold = startThreshold;
        this.expander = expander;
    }
    
    public TraversalBranch next( TraversalContext metadata )
    {
	TraversalBranch result = null;
        while ( result == null )
        {
            if ( current == null ){
                return null;
            } else if (number % threshold == 0 ){
		//System.out.println(current + " "+ current.expanded());
                //current = current.parent();
                //continue;
		return null;
            }
            //System.out.println(number);
            //System.out.println(current);
            TraversalBranch next = current.next( expander, metadata );
	    if ( next == null )
            {
                //number=1;
		current = current.parent();
                continue;
            }
            else {
                current = next;
		this.number++;
            }
            if ( current != null ){
                result = current;
            }
        }
        return result;
    }
}
