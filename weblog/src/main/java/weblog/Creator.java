package weblog;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Creator {
	private static Map<String, Integer> urlmap = new HashMap<String, Integer>();
	private static Map<String, Integer> ipmap = new HashMap<String, Integer>();
	
	private static Map<String, Integer> ip_decmap = new HashMap<String, Integer>();
	private static Map<String, Integer> url_decmap = new HashMap<String, Integer>();
	private static Map<String, Integer> ip_statmap = new HashMap<String, Integer>();
	private static Map<String, Integer> url_statmap = new HashMap<String, Integer>();
	private static Map<String, Integer> ip_cntmap = new HashMap<String, Integer>();

	private static ValueComparator ubvc = new ValueComparator(urlmap);
	private static ValueComparator ibvc = new ValueComparator(ipmap);

	private static TreeMap<String, Integer> sorUrlmap = new TreeMap<String, Integer>(ubvc);
	private static TreeMap<String, Integer> sorIpmap = new TreeMap<String, Integer>(ibvc);
	
	public static void main(String[] args) throws Exception {
		TotalMerge merge = new TotalMerge();
		merge.countCounter();
		merge.countDetector();
		merge.countState();
		merge.sortMap();
		merge.printMap();
		
		Make m = new Make();
		m.makeIndex();
	}

	public static class Make {
		private BufferedReader indexhead;
		private BufferedReader indextail;
		private BufferedReader bartem;
		
		private BufferedWriter index;
		private BufferedWriter bar;
		
		public void makeIndex() throws Exception {
			indexhead = new BufferedReader(new FileReader("/var/www/html/index.head"));
			indextail = new BufferedReader(new FileReader("/var/www/html/index.tail"));
			index = new BufferedWriter(new FileWriter("/var/www/html/index.js"));
			
			String s = null;
			while((s = indexhead.readLine()) != null) {
				index.write(s);
			}
			
			int count = 0;
			Set<String> keys = sorUrlmap.keySet();
			Iterator<String> it = keys.iterator();
			while(it.hasNext()) {
				String key = it.next();
				int n = sorUrlmap.get(key);
				index.write("{text: \"" + key + ", count: \"" + n + "\"},");
				if(count == 9) break;
				count++;
			}
			
			while((s = indextail.readLine()) != null) {
				index.write(s);
			}
			indexhead.close();
			indextail.close();
			index.close();
		}
	}
	
	public static class TotalMerge {
		private BufferedReader counter;
		private BufferedReader detector;
		private BufferedReader state;

		public void countDetector() throws Exception {
			detector = new BufferedReader(new FileReader("detector.txt"));
			
			String s = null;
			while ((s = detector.readLine()) != null) {
				String words[] = s.split("\\s+");
				String ip = words[0];
				String url = words[3];
				int val = Integer.parseInt(words[4]) * 3;

				/* ipmap */
				int inum = 0;
				try { inum = ip_decmap.get(ip); } 
				catch (java.lang.NullPointerException e) { inum = 0; }
				if (inum == 0) { ip_decmap.put(ip, val); } 
				else { ip_decmap.put(ip, inum + val); }
				
				try { inum = ipmap.get(ip); } 
				catch (java.lang.NullPointerException e) { inum = 0; }
				if (inum == 0) { ipmap.put(ip, val); } 
				else { ipmap.put(ip, inum + val); }

				/* urlmap */
				int unum = 0;
				try { unum = url_decmap.get(url); } 
				catch (java.lang.NullPointerException e) { unum = 0; }
				if (unum == 0) { url_decmap.put(url, val); } 
				else { url_decmap.put(url, unum + val); }
				
				try { unum = urlmap.get(url); } 
				catch (java.lang.NullPointerException e) { unum = 0; }
				if (unum == 0) { urlmap.put(url, val); } 
				else { urlmap.put(url, unum + val); }
			}			
			
			detector.close();
		}

		public void countState() throws Exception {
			state = new BufferedReader(new FileReader("state.txt"));
			
			String s = null;
			while ((s = state.readLine()) != null) {
				String words[] = s.split("\\s+");
				String ip = words[0];
				String url = words[1];
				int val = Integer.parseInt(words[2]) * 2;

				/* ipmap */
				int inum = 0;
				try { inum = ip_statmap.get(ip); } 
				catch (java.lang.NullPointerException e) { inum = 0; }
				if (inum == 0) { ip_statmap.put(ip, val); } 
				else { ip_statmap.put(ip, inum + val); }
				
				try { inum = ipmap.get(ip); } 
				catch (java.lang.NullPointerException e) { inum = 0; }
				if (inum == 0) { ipmap.put(ip, val); } 
				else { ipmap.put(ip, inum + val); }

				/* urlmap */
				int unum = 0;
				try { unum = url_statmap.get(url); } 
				catch (java.lang.NullPointerException e) { unum = 0; }
				if (unum == 0) { url_statmap.put(url, val); } 
				else { url_statmap.put(url, unum + val); }
				
				try { unum = urlmap.get(url); } 
				catch (java.lang.NullPointerException e) { unum = 0; }
				if (unum == 0) { urlmap.put(url, val); } 
				else { urlmap.put(url, unum + val); }
			}
			state.close();
		}

		public void countCounter() throws Exception {
			counter = new BufferedReader(new FileReader("counter.txt"));
			
			String s = null;
			while ((s = counter.readLine()) != null) {
				String words[] = s.split("\\s+");
				String ip = words[0];
				int val = Integer.parseInt(words[3]);

				/* 초당 요청 횟수가 100회 이상인 경우 */
				int inum = 0;
				if (val > 100) {
					try { inum = ip_cntmap.get(ip); } 
					catch (java.lang.NullPointerException e) { inum = 0; }
					if (inum == 0) { ip_cntmap.put(ip, 1); } 
					else { ip_cntmap.put(ip, inum + 1); }
					
					try { inum = ipmap.get(ip); } 
					catch (java.lang.NullPointerException e) { inum = 0; }
					if (inum == 0) { ipmap.put(ip, 1); } 
					else { ipmap.put(ip, inum + 1); }
				}
			}
			counter.close();
		}

		public void sortMap() {
			sorUrlmap.putAll(urlmap);
			sorIpmap.putAll(ipmap);
		}

		public void printMap() {
			System.out.println("========== URL ==========");
			int count = 0;
			Set<String> keys = sorUrlmap.keySet();
			Iterator<String> it = keys.iterator();
			while(it.hasNext()) {
				String key = it.next();
				System.out.println(key + " : " + sorUrlmap.get(key));
				if(count == 9) break;
				count++;
			}
			System.out.println("=========================\n");

			System.out.println("========== IP ==========");
			count = 0;
			Set<String> ikeys = sorIpmap.keySet();
			Iterator<String> iit = ikeys.iterator();
			while(iit.hasNext()) {
				String key = iit.next();
				System.out.println(key + " : " + sorIpmap.get(key));
				if(count == 9) break;
				count++;
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
