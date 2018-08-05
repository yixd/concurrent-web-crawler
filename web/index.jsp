<%--
  Created by IntelliJ IDEA.
  User: yixd
  Date: 10/26/16
  Time: 6:10 AM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
  <head>
    <title>Geegle</title>
  </head>
  <body>
  <center>
    <h1>Geegle now!</h1>
  </center>
  <%
      String site = new String("http://localhost:8080/myapp");
      response.setStatus(response.SC_MOVED_TEMPORARILY);
      response.setHeader("Location", site);
  %>
  </body>
</html>
