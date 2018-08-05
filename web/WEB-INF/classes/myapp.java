/**
 * Created by yixd on 10/26/16.
 */
import java.io.*;
import javax.servlet.*;
import javax.servlet.http.*;
import util.HTMLFilter;
import java.sql.*;

public class myapp extends HttpServlet {
    public void doGet(HttpServletRequest request, HttpServletResponse
            response)
            throws IOException, ServletException
    {

        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.println("<html>");
        out.println("<head>");
        out.println("<title>Geegle now!</title>");
        out.println("</head>");
        out.println("<body>");
        out.println("<h3>Geegle now!</h3>");
        out.println("Parameters in this request:<br>");
        String key1 = request.getParameter("key1");
        String key2 = request.getParameter("key2");
        if (key1 != null && key2 != null) {
            out.println("Keyword 1:");
            out.println(" = " + HTMLFilter.filter(key1) +
                    "<br>");
            out.println("Keyword 2:");
            out.println(" = " + HTMLFilter.filter(key2) + "<br><br>");
            if(key1.isEmpty() && !key2.isEmpty()) {
                key1 = key2;
                key2 = "purdue";
            } else if(!key1.isEmpty() && key2.isEmpty()) {
                key2 = "purdue";
            }
            try {
                Class.forName("com.mysql.jdbc.Driver");
                int id = 0;
                Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/crawler", "root", "823917");
                Connection conn2 = DriverManager.getConnection("jdbc:mysql://localhost:3306/crawler", "root", "823917");
                PreparedStatement pstmt = conn.prepareStatement("select url, description, urlid from URLS natural join WORDS where word = ? and description not like \"img\" and urlid in (select urlid from WORDS where word = ?)");
                pstmt.setString(1, key1);
                pstmt.setString(2, key2);
                ResultSet rs = pstmt.executeQuery();
                rs.last();
                out.println(rs.getRow() + " results are found<br>");
                rs.beforeFirst();
                int i = 0;
                while(rs.next()) {
                    out.println(++i + ": <a href=\"" + rs.getString(1) + "\"> " + rs.getString(1) + "</a> ( "
                    + rs.getString(2) + " ) <br>");
                    id = rs.getInt(3);
                    PreparedStatement pstmt2 = conn2.prepareStatement("select url from URLS where urlid = ? and description like ?");
                    pstmt2.setInt(1, id);
                    pstmt2.setString(2, "img");
                    ResultSet rs2 = pstmt2.executeQuery();
                    if(rs2.next()) {
                        //out.println("img: " + rs2.getString(1) + "<br>");
                        out.println("<a href=\"" + rs.getString(1) + "\"><img src=\"" + rs2.getString(1) + "\" style=\"width:100px;height:100px;\"></a><br>");
                    }else{
                        //out.println("NO IMAGE<br>");
                    }
                    pstmt2.close();
                    rs2.close();
                }
                //pstmt.close();
                //rs.close();

                pstmt.close();
                rs.close();
                conn.close();
            }catch(SQLException e) {
                out.println("what just happened");
                e.printStackTrace();
            }catch(ClassNotFoundException e2) {
                e2.printStackTrace();
            }
        }else{
            out.println("No Parameters, Please enter some");
        }
        out.println("<P>");
        out.print("<form action=\"");
        out.print("myapp\" ");
        out.println("method=POST>");
        out.println("Keyword 1:");
        out.println("<input type=text size=20 name=key1>");
        out.println("<br>");
        out.println("Keyword 2:");
        out.println("<input type=text size=20 name=key2>");
        out.println("<br>");
        out.println("<input type=submit>");
        out.println("</form>");
        out.println("</body>");
        out.println("</html>");
    }
    public void doPost(HttpServletRequest request, HttpServletResponse res)
            throws IOException, ServletException
    {
        doGet(request, res);
    }
}
