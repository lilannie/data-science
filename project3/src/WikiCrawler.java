import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
* Crawl Wikipedia pages creating a directed graph with edges stored in output file
* Traverse a web graph using a BFS with either a weighted or unweighted queue
* Topic-sensitive crawling, crawl pages only about a particular topic
*/
public class WikiCrawler {

	private static final String BASE_URL = "https://en.wikipedia.org";		// main URL to crawl
	private static final int MAX_WORD_DISTANCE = 20;						// max distance a link should be away from a topic keyword
	private String seedUrl;	 												// original URL to begin crawling
	private int max;  		 												// max number of pages to be crawled
	private String fileName; 												// file name for which graph to be written too
	private ArrayList<String> keywords;										// topic keywords for topic-sensitive crawling
	private boolean isWeighted;												// whether or not our graph is weighted
	private Set<String> robots;												// Wikipedia robots.txt file of sites we don't want to crawl
	
	public WikiCrawler(String seedUrl, String[] keywords, int max, String fileName, boolean isWeighted){
		this.seedUrl = seedUrl;
		this.keywords = new ArrayList<>();
		this.max = max;
		this.fileName = fileName;
		this.isWeighted = isWeighted;
		this.robots = new HashSet<>();
		
		for(String keyword : keywords) {
			this.keywords.add(keyword.toLowerCase());
		}// end for loop adding keywords as lowercase

		BufferedReader br;
		FileReader fr;
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
	
	public void crawl() throws IOException, InterruptedException {
		// writes to the file named <fileName>, first line = number of vertices (max)
		// next lines = a directed edge of the web graph
	    ArrayList<Edge> edges = BFSTraversal(seedUrl);
	    StringBuilder fileContents = new StringBuilder();

		for(Edge edge : edges){
			fileContents.append(edge).append('\r');
		}// end foreach loop over all edges

	    // write the response to a file
	    BufferedWriter bw = null;
		FileWriter fw = null;
		try {
			// begin try-catch block for writing to a file
			fw = new FileWriter(fileName);
			bw = new BufferedWriter(fw);
			bw.write(max + "\n");
			bw.write(fileContents.toString());
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
			} catch (IOException e) {
				e.printStackTrace();
			}// end try-catch block for closing fileWriter

		}// end try-catch-finally block
				
	}// end function crawl()
	
	private ArrayList<String> extractLinks(String doc) throws IOException {
		// parse the string doc and return a list of links from the document (HTML)
		ArrayList<String> links = new ArrayList<>();
		String[] lines = doc.split("\r");

		for(String line : lines){
			// for loop over all our lines
			String regex = "href=\"/wiki/.*?\"";
			Pattern string = Pattern.compile(regex);
		    Matcher m = string.matcher(line);

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
		WeightedQueue<Tuple<String>> queue = new WeightedQueue<>();
		
		// some lists we need here
		HashSet<String> visited = new HashSet<>();
		ArrayList<String> extractedLinks;
		ArrayList<Edge> edges = new ArrayList<>();
		
		// make an initial GET request to seed url
		String response = request(seed_url);

		// keep us under max requests by counting the number of requests
		int requestCounter = 1;
		int linkCounter = 1;
		
		// begin by adding our seedUrl to the queue and visited
		queue.add(new Tuple<>(seed_url, weight(seed_url, response), linkCounter));
		visited.add(seed_url);
		
		while(!queue.isEmpty()){
			// while loop over all links in the queue
			String currentPage = queue.extract().item;

			// make a GET request to the currentPage
			response = request(currentPage);

			if(++requestCounter % 10 == 0){
				Thread.sleep(1000);
			}// end if we need to sleep
		    
		    // extract our links from our currentPage Response
		    extractedLinks = extractLinks(response);

			for(String link : extractedLinks){
		    	// for loop over all our extracted links
		    	if(!visited.contains(link) && visited.size() < max && !robots.contains(link)) {
		    		queue.add(new Tuple<>(link, weight(link, response), ++linkCounter));
		    		visited.add(link);
	    		}// end if we should visit this link
		    	
		    	// add the edge to our wiki graph
	    		Edge e = new Edge(currentPage, link);
	    		if(visited.contains(link) && !currentPage.equals(link) && !edges.contains(e)) {
	    			edges.add(e);
	    		}// end if we should add this edge to our graph
		    	
		    }// end for loop over all our extracted links		    		

		}// end while loop over all items in the queue
		
		return edges;
	}// end function BFSTraversal()
	
	private double weight(String link, String response) {
		// compute weight for a link within a space according to topic keywords
		if(!isWeighted) {
			return 0;
		}// end if not weighted
		
		int minDistance = Integer.MAX_VALUE;
		ArrayList<String> responseWords = new ArrayList<>(Arrays.asList(response.split(" ")));
		int link_index = responseWords.indexOf(link);
		
		while(link_index > 0) {
			// grab the first occurrence of the link in the response
			link_index = responseWords.indexOf(link);
			int distance = 1;
			boolean reverse = false;
			int index = link_index;
			
			while(distance <= MAX_WORD_DISTANCE) {
				// loop until our link is too far away from a keyword
				if(index > responseWords.size()) {
					index = link_index;
					reverse = true;
					distance = 0;
				}else if(index < 0) {
					break;
				}// end if checking where we are in the body of the response
				 
				String word = responseWords.get(index).toLowerCase();
				
				if(keywords.contains(word) && distance <= minDistance){
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

	private String request(String url) throws IOException {
		// initialize streams and readers
		InputStream is;
		BufferedReader rd;
		StringBuilder response = new StringBuilder();

		try {
			// open GET request to our URL
			is = new URL(BASE_URL + url).openStream();
			rd = new BufferedReader(new InputStreamReader(is));
		} catch (Exception e) {
			e.printStackTrace();
			return response.toString();
		}// end try-catch block

		// append the entire request to a string
		String line;
		boolean storeResponse = false;
		while ((line = rd.readLine()) != null) {

			if(line.contains("<p>") || line.contains("<P>")){
				// we don't care about links until we reach first <p>
				storeResponse = true;
			}// end if we've reached the first paragraph tag, begin parsing links

			if(storeResponse){
				response.append(line).append('\r');
			}// end if we are parsing links

		}// end while we are reading the line creating our response

		is.close();
		rd.close();
		return response.toString();
	}// end function request
	
	public static void main(String[] args) throws InterruptedException, IOException {
		String[] topics = {"tennis", "grand slam"};
		WikiCrawler w = new WikiCrawler("/wiki/tennis", topics, 1000, "WikiTennisGraph.txt", true);
		w.crawl();
	}// end main function
	
}// end class WikiCrawler