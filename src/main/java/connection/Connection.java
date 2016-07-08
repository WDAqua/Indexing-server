package connection;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.io.Serializable;

public class Connection implements Serializable{
	private HashMap<ArrayList<Integer>, ArrayList<Integer>> list = new HashMap<ArrayList<Integer>, ArrayList<Integer>>();
	
	public void add(int i, int j, int k){
		ArrayList<Integer> s = new ArrayList<Integer>();
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
	
	public boolean equals(int i, int j, int k){
		ArrayList<Integer> s = new ArrayList<Integer>();
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
