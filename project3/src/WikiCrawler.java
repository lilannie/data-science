import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WikiCrawler {
	/* crawl Wikipedia pages creating a directed graph with edges stored in WikiCS.txt */
	
	private static final String BASE_URL = "https://en.wikipedia.org";		// main URL to crawl
	private static final int MAX_WORD_DISTANCE = 20;						// max distance a link should be away from a topic keyword
	private String seedUrl;	 												// original URL to begin crawling
	private int max;  		 												// max number of pages to be crawled
	private String fileName; 												// file name for which graph to be written too
	private ArrayList<String> keywords;										// topic keywords for topic-sensitive crawling
	private boolean isWeighted;												// whether or not our graph is weighted
	private Set<String> robots;
	
	public WikiCrawler(String seedUrl, String[] keywords, int max, String fileName, boolean isWeighted){
		this.seedUrl = seedUrl;
		this.keywords = new ArrayList<String>();
		
		for(String keyword : keywords) {
			this.keywords.add(keyword.toLowerCase());
		}// end for loop adding keywords as lowercase
		
		this.max = max;
		this.fileName = fileName;
		this.isWeighted = isWeighted;
		this.robots = new HashSet<>();
		
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader("robots.txt");
			br = new BufferedReader(fr);
			String line;

			while ((line = br.readLine()) != null) {
				Scanner s = new Scanner(line);
			
				if(s.hasNext() && s.next().equals("Disallow:") && s.hasNext()) {
					robots.add(s.next());
				}// end if this is a link we dont want to crawl
				
				s.close();
			}// end while over each line in robots.txt

		} catch (IOException e) {
			e.printStackTrace();
		}// end try catch block reading robots.txt
		
	}// end WikiCrawler constructor
	
	public void crawl() throws MalformedURLException, IOException, InterruptedException {
		// writes to the file named <fileName>, first line = number of vertices (max)
		// next lines = a directed edge of the web graph
	    ArrayList<Edge> edges = BFSTraversal(seedUrl);
		String fileContents = "";
		
		for(int i = 0; i < edges.size(); i++){
			fileContents = fileContents + edges.get(i).toString() + "\r";
		}// end for loop over all edges
	    
	    // write the response to a file
	    BufferedWriter bw = null;
		FileWriter fw = null;

		try {
			// begin try-catch block for writing to a file
			fw = new FileWriter(fileName);
			bw = new BufferedWriter(fw);
			bw.write(max + "\n");
			bw.write(fileContents);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// begin finally block for closing 
			try {
				// begin try-catch block for closing fileWriter
				if (bw != null)
					bw.close();
				if (fw != null)
					fw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}// end try-catch block for closing fileWriter

		}// end try-catch-finally block
				
	}// end function crawl()
	
	public ArrayList<String> extractLinks(String doc) throws IOException {
		// parse the string doc and return a list of links from the document (HTML)
		ArrayList<String> links = new ArrayList<String>();	
		String[] lines = doc.split("\r");
		
		for(int i = 0; i < lines.length; i++){
			// for loop over all our lines
			String regex = "href=\"/wiki/.*?\"";
			Pattern string = Pattern.compile(regex);
		    Matcher m = string.matcher(lines[i]);

		    while (m.find()) {
		    	// while loop over all our link matches
		    	String link = m.group().substring(m.group().indexOf('"')+1, m.group().length()-1);
		    	
		    	if(!link.contains("#") && !link.contains(":") && !links.contains(link)){
		    		 // only add links that don't contain these strings, don't add duplicates
		    		 links.add(link);
		    	}// end if only add certain links
		    	
		    }// end while loop over regex matches
		    
		}// end for loop over all lines in the document
		
		return links;
	}// end function extractLinks(...)
	
	private ArrayList<Edge> BFSTraversal(String seed_url) throws InterruptedException, IOException {
		// traverse the web graph starting at seed_url
		WeightedQueue<Tuple<String>> queue = new WeightedQueue<Tuple<String>>();
		
		// some lists we need here
		HashSet<String> visited = new HashSet<String>();
		ArrayList<String> extractedLinks = new ArrayList<String>();
		ArrayList<Edge> edges = new ArrayList<Edge>();
		
		// initialize streams and readers
		InputStream is = null;
		BufferedReader rd = null;
		StringBuffer response = null;
		
		try {
			// open GET request to our URL			    
		    is = new URL(BASE_URL + seed_url).openStream();
		    rd = new BufferedReader(new InputStreamReader(is));
		    response = new StringBuffer();
		} catch (Exception e) {
	      e.printStackTrace();
		}// end try-catch block
		
		// keep us under max requests by counting the number of requests
		int requestCounter = 1;
		int linkCounter = 1;
		
		// begin by adding our seedUrl to the queue and visited
		queue.add(new Tuple<String>(seed_url, weight(seed_url, response.toString()), linkCounter));
		visited.add(seed_url);
		
		while(!queue.isEmpty()){
			// while loop over all links in the queue
			String currentPage = queue.extract().item;
			
			// boolean for determining if we parse links, set true after first <p> tag
			boolean linkParse = false;
			try {
				// open GET request to our URL			    
			    is = new URL(BASE_URL + currentPage).openStream();
			    rd = new BufferedReader(new InputStreamReader(is));
			    response = new StringBuffer();
			    // increment our request counter
			    requestCounter++;
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}// end try-catch block
					
			if(requestCounter % 10 == 0){
				Thread.sleep(1000);
			}// end if we need to sleep
			
		    // append our entire response to a single String
		    String line;
		    while ((line = rd.readLine()) != null) {
		    	
		    	if(line.contains("<p>") || line.contains("<P>")){
		    		// we don't care about links until we reach first <p>
		    		linkParse = true;
		    	}// end if we've reached the first paragraph tag, begin parsing links
		    	
		    	if(linkParse){
		    		response.append(line);
		    		response.append('\r');
		    	}// end if we are parsing links
		     
		    }// end while we are reading the line creating our response
		    
		    // extract our links from our currentPage Response
		    extractedLinks = extractLinks(response.toString());
		    
		    for(int i = 0; i < extractedLinks.size(); i++){
		    	// for loop over all our extracted links
		    	String link = extractedLinks.get(i);
		    	
		    	if(!visited.contains(link) && visited.size() < max && !robots.contains(link)) {
		    		queue.add(new Tuple<String>(link, weight(link, response.toString()), ++linkCounter));
		    		visited.add(link);
	    		}// end if we should visit this link
		    	
		    	// add the edge to our wiki graph
	    		Edge e = new Edge(currentPage, link);
	    		if(visited.contains(link) && !currentPage.equals(link) && !edges.contains(e)) {
	    			edges.add(e);
	    		}// end if we should add this edge to our graph
		    	
		    }// end for loop over all our extracted links		    		

		   if(rd != null)
			   rd.close();
		   if(is != null)
			   is.close();
		}// end while loop over all items in the queue
		
		return edges;
	}// end function BFSTraversal()
	
	private double weight(String link, String response) {
		if(!isWeighted) {
			return 0;
		}// end if not weighted
		
		int minDistance = Integer.MAX_VALUE;
		ArrayList<String> responseWords = new ArrayList<String>(Arrays.asList(response.split(" ")));
		int link_index = responseWords.indexOf(link);
		
		while(link_index > 0) {
			// grab the first occurence of the link in the response
			link_index = responseWords.indexOf(link);
			int distance = 1;
			boolean reverse = false;
			int index = link_index;
			
			while(distance <= MAX_WORD_DISTANCE)
			{
				if(index > responseWords.size()) {
					index = link_index;
					reverse = true;
					distance = 0;
				}else if(index < 0) {
					break;
				}// end if checking where we are in the body of the response
				 
				String word = responseWords.get(index).toLowerCase();
				
				if(keywords.contains(word) && distance < minDistance){
					minDistance = distance;
					distance = MAX_WORD_DISTANCE;
				}// end if the current word contains the keyword
				
				if(distance == MAX_WORD_DISTANCE && !reverse) {
					index = link_index;
					reverse = true;
					distance = 0;
				}// end if distance == 20 and we haven't went in the reverse direction
				
				if(reverse){
					index--;
				}else{
					index++;
				}// end if searching for distance in reverse
				
				distance++;
			}// end while distance is less than or equal to MAX_WORD_DISTANCE
			
			// remove this link from our list of response words
			responseWords.remove(link_index);
		}// end while loop over each duplicate link in the response
		
		if(minDistance > MAX_WORD_DISTANCE) {
			return 0;
		}else {
			return 1/(minDistance + 2);
		}// end if distance > 20
		
	}// end function weight
	
	class Edge {
		// create an Edge of a directed graph (<start>, <end>) pair
		String start;
		String end;
		
		public Edge(String start, String end){
			this.start = start;
			this.end = end;
		}// end Edge constructor
			
		@Override
		public String toString(){
			return start + " " + end;
		}// end function toString()
		
		@Override
		public boolean equals(Object o){
			Edge e = (Edge) o;
			return this.start == e.start && this.end == e.end;
		}// end function equals()
		
	}// end class Edge
	
	public static void main(String[] args) throws InterruptedException, MalformedURLException, IOException {
		String[] topics = {"tennis", "grand slam"};
		WikiCrawler w = new WikiCrawler("/wiki/tennis", topics, 1000, "WikiTennisGraph.txt", true);
		w.crawl();
	}// end main function
	
}// end class WikiCrawler