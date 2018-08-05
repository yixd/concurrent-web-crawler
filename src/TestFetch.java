import java.io.InputStreamReader;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
/**
 * Created by yixd on 10/28/16.
 */
public class TestFetch {
    public static void main(String args[]) {
        try {
            String urlScanned = "http://www.cs.purdue.edu";
            URL url = new URL(urlScanned);
            System.out.println("urlscanned=" + urlScanned + " url.path=" + url.getPath());
            // open reader for URL
            InputStreamReader in = new InputStreamReader(url.openStream());
            // read contents into string builder
            StringBuilder input = new StringBuilder();
            int ch;
            while ((ch = in.read()) != -1) {
                input.append((char) ch);
            }

            // search for all occurrences of pattern
            //String patternString =  "<a\\s+href\\s*=\\s*(\"[^\"]*\"|[^\\s>]*)\\s*>";

            String patternString = "<a\\s(\\w+=\"[^\"]*\"\\s)*href=(\"([^\"]*)\")[^>]*>";
            //String patternString = "<a\\s+href=(\"([^\"]*)\")\\s*>";
            Pattern pattern = Pattern.compile(patternString, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(input);
            int i = 0;
            System.out.println("yoyoyo");
            while (matcher.find()) {
                String match = input.substring(matcher.start(), matcher.end());
                String urlFound = matcher.group(3);
                System.out.println("urlFound(" + i++ + "): " + urlFound);
                //System.out.println("match: " + match);
            }

            //compare with jsoup
            Document doc = Jsoup.connect(urlScanned).get();
            Elements links = doc.select("a[href]");
            int j = 0;
            for (Element link : links) {
                System.out.printf(" * a[" + j++ + "]: <%s>  (%s)\n", link.attr("abs:href"), link.text());
            }
            System.out.println("i = " + i + "; j = " + j);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }
}
