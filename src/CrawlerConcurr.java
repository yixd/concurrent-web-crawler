/**
 * Created by yixd on 10/26/16.
 */
import java.io.*;
import java.net.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.*;
import java.sql.*;
import java.util.*;

import com.sun.xml.internal.ws.encoding.StringDataContentHandler;
import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
public class CrawlerConcurr {
    long initial_time;
    Connection connection;
    private AtomicInteger urlID;
    private AtomicInteger left;
    int MAXURLS;
    static int TREM_LEN;
    public Properties props;
    private Queue<String> queue;
    private ConcurrentHashMap<String, String> urlBank;
    public HashSet<String> stopwords;
    //properties
    String jdbcurl;
    String username;
    String password;
    String DOMAIN;

    CrawlerConcurr() throws IOException, SQLException {
        initial_time = System.currentTimeMillis();
        TREM_LEN = 100;
        urlID = new AtomicInteger(0);
        MAXURLS = 1000;
        stopwords = new HashSet<>();
        DOMAIN = null;
        queue = new ConcurrentLinkedDeque<>(); //queue of links waiting to be crawled
        urlBank = new ConcurrentHashMap<>(); //store normalized link to avoid duplicate links
        readProperties();
        connection = openConnection();
        left = new AtomicInteger(MAXURLS);
        createDB();
        String root[] = props.getProperty("crawler.url-list").split(",");
        for (String s : root) {
            if(s != null && !s.isEmpty()) {
                queue.add(s);
                //urlBank.add(normalize(s));
            }
        }
        Document doc = Jsoup.connect("http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words").get();
        String text = doc.body().text().toLowerCase();
        String[] tokens = text.trim().split(" ");
        for (String s : tokens) {
            stopwords.add(s);
        }
    }

    public void readProperties() throws IOException {
        props = new Properties();
        FileInputStream in = new FileInputStream("database.properties");
        props.load(in);
        in.close();
        String drivers = props.getProperty("jdbc.drivers");
        if (drivers != null) System.setProperty("jdbc.drivers", drivers);

        jdbcurl = props.getProperty("jdbc.url");
        username = props.getProperty("jdbc.username");
        password = props.getProperty("jdbc.password");
        if(props.getProperty("crawler.domain") != null) {
            DOMAIN = props.getProperty("crawler.domain");
        }
        if(props.getProperty("crawler.maxurls") != null){
            MAXURLS = Integer.parseInt(props.getProperty("crawler.maxurls"));
        };
    }

    public Connection openConnection() throws SQLException, IOException
    {
        return DriverManager.getConnection( jdbcurl, username, password);
    }

    public void createDB() throws SQLException, IOException {
        Statement stat = connection.createStatement();
        // Delete the table first if any
        try {
            stat.executeUpdate("DROP TABLE URLS");
            stat.executeUpdate("DROP TABLE WORDS");
        }
        catch (Exception e) {
        }
        // Create the table
        stat.executeUpdate("CREATE TABLE URLS (urlid INT, url VARCHAR(512), description VARCHAR(200))");
        stat.executeUpdate("CREATE TABLE WORDS (word varchar(512), urlid int)");
    }

    public boolean domainChk (String url){
        if (DOMAIN == null || DOMAIN.isEmpty()) {
            return true;
        } else if(url != null && url.contains(DOMAIN)) {
            return true;
        } else {
            return false;
        }
    }

    public static String ntrim(String s, int len) {
        if(s.length() > len) {
            return s.substring(0, len - 1) + ".";
        } else {
            return s;
        }
    }
    public String getDesc(String url) throws IOException {
        Document doc = Jsoup.connect(url)
                .userAgent("Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36")
                .get();
        String desc = doc.head().text();
        if(desc.isEmpty()) {
            doc.select("script, comment, #comment").remove();
            return doc.select("p").text();
        } else {
            return doc.head().text();
        }
    }
    public String normalize (String url) {
        if(url != null && !url.isEmpty()) {
            if (url.startsWith("http://www.")) {
                url = url.replaceFirst("http://www.", "");
            } else if (url.startsWith("https://www.")) {
                url = url.replaceFirst("https://www.", "");
            } else if (url.startsWith("http://")) {
                url = url.replaceFirst("http://", "");
            } else if (url.startsWith("https://")) {
                url = url.replaceFirst("https://", "");
            } else if (url.startsWith("www.")) {
                url = url.replaceFirst("www.", "");
            }
            if (url.contains("#")) {
                url = url.substring(0, url.indexOf('#'));
            }
            if (url.endsWith("/")) {
                url = url.substring(0, url.length() - 1);
            }
            if (url.endsWith("index.html")) {
                url = url.substring(0, url.indexOf("index.html"));
            }
            if (url.endsWith("/")) {
                url = url.substring(0, url.length() - 1);
            }
            if (url.contains("mailto:") || url.contains("ftp:") ||
                    url.contains("file:")) {
                url = null;
            }
            return url;
        } else {
            return null;
        }
    }
    public void dfs() throws IOException, SQLException, URISyntaxException {
        HashSet<String> normset = new HashSet<>();
        Connection dfsconn = openConnection();
        dfsconn.setAutoCommit(false);
        PreparedStatement pstmt = dfsconn.prepareStatement("insert into URLS values (?, ?, ? )");

        while(urlID.get() < MAXURLS && !queue.isEmpty()) {
            String url = queue.remove();
            String norm = normalize(url);
            //if (url != null && !url.isEmpty() && domainChk(url) && !urlBank.containsKey(norm) && !urlInDB(url)) {
            try {
                if (url != null && norm != null && !norm.isEmpty() && !normset.contains(norm) && domainChk(url)) {
                    //if (url != null && !url.isEmpty() && !normset.contains(norm) && domainChk(url)) {
                    // main thread connect and pass doc to thread
                    normset.add(norm);
                    Document doc = Jsoup.connect(url).get();
                    Elements links = doc.select("a[href]");
                    for (Element link : links) {
                        queue.add(link.attr("abs:href"));
                    }
                    //store image
                    Element img = doc.select("img[src$=.png], img[src$=.jpg], img[src$=.gif]").first();
                    if(img != null) {
                        String imgurl = img.absUrl("src");
                        if(imgurl != null && !imgurl.isEmpty()) {
                            pstmt.setInt(1, urlID.get());
                            pstmt.setString(2, imgurl);
                            pstmt.setString(3, "img");
                            pstmt.addBatch();
                        }
                    }
                    // start thread
                    new Thread(new Parse(doc, urlID.get())).start();
                    //send to patch
                    pstmt.setInt(1, urlID.get());
                    pstmt.setString(2, url);
                    pstmt.setString(3, ntrim(getDesc(url), 100));
                    pstmt.addBatch();
                    System.out.print(".");
                    //insertURLInDB(urlID.get(), norm, ntrim(getDesc(url), 100));
                    urlID.getAndIncrement();
                    //urlBank.put(norm, url);
                }
            }catch(org.jsoup.UnsupportedMimeTypeException e){

            }catch (java.net.MalformedURLException e2) {
                //ignore
                System.out.println("\nbad: " + url);
            }catch(java.net.SocketTimeoutException e3) {
                System.out.println("\ntimeout: " + url);
            }catch(org.jsoup.HttpStatusException e4) {
                System.out.println("\nstatus: " + url);
            }catch(java.net.UnknownHostException e5) {
                e5.printStackTrace();
            }catch(Exception ef) {
                ef.printStackTrace();
            }
        }
        pstmt.executeBatch();
        dfsconn.commit();
        dfsconn.close();
    }

    public class Parse implements Runnable {
        private Document doc;
        private int id;
        public Parse(Document doc, int id) {
            this.doc = doc;
            this.id = id;
        }

        @Override
        public void run() {
            Connection local = null;
            try {
                local = openConnection();
                local.setAutoCommit(false);
                PreparedStatement pstmt = connection.prepareStatement("insert into WORDS values (?, ? )");
                //create local hashmap
                HashSet<String> hashtok = new HashSet<>();
                //extract words and add to database
                long local_init = System.currentTimeMillis();
                String[] tokens = doc.body().text()
                        .replaceAll("[^a-z\\sA-Z]","")
                        .toLowerCase().trim()
                        .split(" ");
                for(String tok : tokens) {
                    if(!tok.isEmpty() && !stopwords.contains(tok) && !hashtok.contains(tok)) {
                        hashtok.add(tok);
                        pstmt.setString(1, tok);
                        pstmt.setInt(2, id);
                        pstmt.addBatch();
                    }
                }
                pstmt.executeBatch();
                local.commit();
                local.close();
                System.out.println("\nleft: " + left.decrementAndGet() + " urlid: " + id + " single time: "  + (System.currentTimeMillis()-local_init)/1000f +  " total time : "+(System.currentTimeMillis()-initial_time)/1000f+" sec ");
            } catch (Exception e)  {
                e.printStackTrace();
            } finally {
                if(local != null) {
                    try {
                        local.close();
                    }catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void main(String[] args)
    {
        try {
            //long a = System.currentTimeMillis();
            CrawlerConcurr crawler = new CrawlerConcurr();
            crawler.dfs();
            //crawler.printDB("URLS");
            //crawler.printDB("WORDS");
        }
        catch( Exception e) {
            e.printStackTrace();
        }
    }
}

