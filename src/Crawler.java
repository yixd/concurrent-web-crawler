/**
 * Created by yixd on 10/26/16.
 */
import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.sql.*;
import java.util.*;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
public class Crawler {
    Connection connection;
    int urlID;
    int MAXURLS;
    static int TREM_LEN;
    String DOMAIN;
    public Properties props;
    private Queue<String> queue;
    private HashSet<String> urlBank;

    Crawler() throws IOException, SQLException {
        TREM_LEN = 100;
        urlID = 0;
        MAXURLS = 1000;
        DOMAIN = null;
        queue = new LinkedList<>();
        urlBank = new HashSet<>();
        readProperties();
        createDB();
        String root[] = props.getProperty("crawler.url-list").split(",");
        for (String s : root) {
            if(s != null && !s.isEmpty()) {
                queue.add(s);
                //urlBank.add(normalize(s));
            }
        }
    }

    public void readProperties() throws IOException {
        props = new Properties();
        FileInputStream in = new FileInputStream("database.properties");
        props.load(in);
        in.close();
    }

    public void openConnection() throws SQLException, IOException
    {
        String drivers = props.getProperty("jdbc.drivers");
        if (drivers != null) System.setProperty("jdbc.drivers", drivers);

        String url = props.getProperty("jdbc.url");
        String username = props.getProperty("jdbc.username");
        String password = props.getProperty("jdbc.password");
        if(props.getProperty("crawler.domain") != null) {
            DOMAIN = props.getProperty("crawler.domain");
        }
        if(props.getProperty("crawler.maxurls") != null){
            MAXURLS = Integer.parseInt(props.getProperty("crawler.maxurls"));
        };
        connection = DriverManager.getConnection( url, username, password);
    }

    public void createDB() throws SQLException, IOException {
        openConnection();
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
    public void printDB(String db) throws SQLException{
        Statement stat = connection.createStatement();
        ResultSet result = stat.executeQuery("SELECT * FROM " + db);
        ResultSetMetaData rsmd = result.getMetaData();
        int cols = rsmd.getColumnCount();
        for (int i = 1; i <= cols; i++) {
            System.out.printf(rsmd.getColumnName(i) + " ");
        }
        System.out.printf("\n");
        while(result.next()) {
            for (int i = 1; i <= cols; i++) {
                System.out.printf(result.getString(i) + " ");
            }
            System.out.printf("\n");
        }
        result.close();
    }
    public boolean urlInDB(String urlFound) throws SQLException, IOException {
        Statement stat = connection.createStatement();
        ResultSet result = stat.executeQuery( "SELECT * FROM URLS WHERE url LIKE '"+urlFound+"'");

        if (result.next()) {
            //System.out.println("URL "+urlFound+" already in DB");
            return true;
        }
        // System.out.println("URL "+urlFound+" not yet in DB");
        return false;
    }
    public boolean wordInDB(String word, int id) throws SQLException, IOException {
        Statement stat = connection.createStatement();
        ResultSet result = stat.executeQuery( "SELECT * FROM WORDS WHERE urlid=" + id + " and word LIKE '"+ word +"'");
        if (result.next()) {
            //System.out.println("Word "+word+" already in DB");
            return true;
        }
        // System.out.println("URL "+urlFound+" not yet in DB");
        return false;
    }

    public void insertURLInDB(String url, String desc) throws SQLException, IOException {
        //Statement stat = connection.createStatement();
        //String query = "INSERT INTO URLS VALUES (" + urlID + ",'" + url + "','" + desc +"')";
        //System.out.println("Executing "+query);
        //stat.executeUpdate( query );
        PreparedStatement pstmt = connection.prepareStatement("insert into URLS values (?, ?, ? )");
        pstmt.setInt(1, urlID);
        pstmt.setString(2, url);
        pstmt.setString(3, desc);
        pstmt.executeUpdate();
        System.out.println("insert " + urlID);
        urlID++;
    }
    public void insertWordInDB( String word, int id) throws SQLException, IOException {
        PreparedStatement pstmt = connection.prepareStatement("insert into WORDS values (?, ? )");
        pstmt.setString(1, word);
        pstmt.setInt(2, id);
        pstmt.executeUpdate();
        //System.out.println("insert " + urlID);
    }
    public boolean domainChk (String url) throws URISyntaxException{
        if(DOMAIN == null || DOMAIN.isEmpty()) {
            return true;
        } else {
            String domain = new URI(url).getHost();
            if(domain.startsWith("www"))
                domain = domain.substring(4);
            if (domain.equals(DOMAIN)) {
                return true;
            } else {
                return false;
            }
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
            //System.out.println("normalized url = null : " + (url == null));
            return url;
        } else {
            return null;
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
    public void dfs() throws IOException, SQLException, URISyntaxException {
        while(urlID < MAXURLS && !queue.isEmpty()) {
            String url = queue.remove();
            String norm = normalize(url);
            if (url != null && !url.isEmpty() && domainChk(url) && !urlBank.contains(norm) && !urlInDB(url)) {
                fetchURL(url);
                insertURLInDB(url, ntrim(getDesc(url), 100));
                urlBank.add(norm);
            }
        }
    }

    public void fetchURL(String urlScanned) {
        try {
            Document doc = Jsoup.connect(urlScanned).get();
            //extract links and add to queue
            Elements links = doc.select("a[href]");
            for (Element link : links) {
                queue.add(link.attr("abs:href"));
            }
            //extract words and add to database
            String[] tokens = doc.body().text()
                    .replaceAll("[^a-z\\sA-Z]","")
                    .toLowerCase().trim()
                    .split(" ");
            for(String tok : tokens) {
                if(!tok.isEmpty()) {
                    if (!wordInDB(tok, urlID)) {
                        insertWordInDB(tok, urlID);
                    }
                }
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        try {
            long a=System.currentTimeMillis();
//在最好的一行加上:
            Crawler crawler = new Crawler();
            crawler.dfs();
            //crawler.printDB("URLS");
            //crawler.printDB("WORDS");

            System.out.println("\r<br>执行耗时 : "+(System.currentTimeMillis()-a)/1000f+" 秒 ");
        }
        catch( Exception e) {
            e.printStackTrace();
        }
    }
}

