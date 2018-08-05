import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by yixd on 10/29/16.
 */
public class TestURL {
    public static void main(String[] args) throws URISyntaxException{
        URI uri = new URI("https://www.cs.purdue.edu/index.html");
        String domain = uri.getHost();
        System.out.println("domain: " + domain + (domain.startsWith("www.")? domain.substring(4) : domain));
    }
}
