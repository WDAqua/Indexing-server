import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class Connection{
	private Map<HashSet<Integer>, ArrayList<Integer>> list = new HashMap<HashSet<Integer>, ArrayList<Integer>>();
	
	void add(int i, int j, int k){
		HashSet<Integer> s = new HashSet<Integer>();
		s.add(i);
		s.add(j);
		if (list.containsKey(s)==false){
			ArrayList<Integer> l = new ArrayList<Integer>();
			l.add(k);
			list.put(s, l);
		} else{
			ArrayList<Integer> l=list.get(s);
			l.add(k);
			list.put(s, l);
		}
	}
	
	boolean equals(int i, int j, int k){
		HashSet<Integer> s = new HashSet<Integer>();
		s.add(i);
		s.add(j);
		if (list.containsKey(s)==false && k==0){
			return true;
		} else if (list.containsKey(s)==false && k!=0 ){
			return false;
		} else{
			ArrayList<Integer> l=list.get(s);
			if (l.contains(k)){
				return true;
			} else {
				return false;
			}
		}
		
		
	}
}
