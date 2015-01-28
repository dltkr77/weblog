package weblog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Creator {
	public static void main(String[] args) throws Exception {
		TotalMerge merge = new TotalMerge();
		merge.countCounter();
		merge.countDetector();
		merge.countState();
	}
	
	public static class TotalMerge {
		private FileReader counterfr;
		private FileReader detectorfr;
		private FileReader statefr;
		private BufferedReader counter;
		private BufferedReader detector;
		private BufferedReader state;
		
		private Map<String, Integer> urlmap = new HashMap<String, Integer>();
		private Map<String, Integer> ipmap = new HashMap<String, Integer>();
		
		private ValueComparator bvc =  new ValueComparator(ipmap);
		private TreeMap<String,Integer> sorUrlmap = new TreeMap<String,Integer>(bvc);
		private TreeMap<String,Integer> sorIpmap = new TreeMap<String,Integer>(bvc);
		
		TotalMerge() throws Exception {
			counterfr = new FileReader(new File("counter.txt"));
			detectorfr = new FileReader(new File("detector.txt"));
			statefr = new FileReader(new File("state.txt"));
			
			counter = new BufferedReader(counterfr);
			detector = new BufferedReader(detectorfr);
			state = new BufferedReader(statefr);
		}
	
		public void countDetector() throws Exception {
			String s = null;
			while((s = counter.readLine()) != null) {
				String words[] = s.split("\\s+");
				String ip = words[0];
				String url = words[3];
				int val = Integer.parseInt(words[4]);
				
				/* ipmap */
				int inum = ipmap.get(ip) * 3;
				if(inum == 0) {
					ipmap.put(ip, val);
				} else {
					ipmap.put(ip, inum + val);
				}
				
				/* urlmap */
				int unum = urlmap.get(url) * 3;
				if(unum == 0) {
					urlmap.put(url, val);
				} else {
					urlmap.put(url, unum + val);
				}
			}
		}
		
		public void countState() throws Exception {
			String s = null;
			while((s = state.readLine()) != null) {
				String words[] = s.split("\\s+");
				String ip = words[0];
				String url = words[1];
				int val = Integer.parseInt(words[2]);
				
				/* ipmap */
				int inum = ipmap.get(ip) * 2;
				if(inum == 0) {
					ipmap.put(ip, val);
				} else {
					ipmap.put(ip, inum + val);
				}
				
				/* urlmap */
				int unum = urlmap.get(url) * 2;
				if(unum == 0) {
					urlmap.put(url, val);
				} else {
					urlmap.put(url, unum + val);
				}
			}
		}
		
		public void countCounter() throws Exception {
			String s = null;
			while((s = counter.readLine()) != null) {
				String words[] = s.split("\\s+");
				String ip = words[0];
				int val = Integer.parseInt(words[3]);
				
				/* 초당 요청 횟수가 100회 이상인 경우 */ 
				if(val > 100) {
					int inum = ipmap.get(ip);
					if(inum == 0) {
						ipmap.put(ip, val);
					} else {
						ipmap.put(ip, inum + val);
					}
				}
			}
		}
		
		public void sortMap() {
			sorUrlmap.putAll(urlmap);
			sorIpmap.putAll(ipmap);
		}
		
		public void printMap() {
			System.out.println("========== URL ==========");
			Set<String> keys = sorUrlmap.keySet();
			Iterator<String> it = keys.iterator();
			for(int i = 0; i < 10; i++) {
				String key = it.next();
				System.out.println(key + " : " + sorUrlmap.get(key));
			}
			System.out.println("=========================\n");
			
			System.out.println("========== IP ==========");
			Set<String> ikeys = sorIpmap.keySet();
			Iterator<String> iit = keys.iterator();
			for(int i = 0; i < 10; i++) {
				String key = iit.next();
				System.out.println(key + " : " + sorIpmap.get(key));
			}
			System.out.println("========================");
		}
	}
	
	/* 해쉬맵을 값으로 정렬하기 위한 클래스 */
	public static class ValueComparator implements Comparator<String> {

	    Map<String, Integer> base;
	    public ValueComparator(Map<String, Integer> base) {
	        this.base = base;
	    }
    
	    public int compare(String a, String b) {
	        return (base.get(b) - base.get(a)); 
	    }
	}
}