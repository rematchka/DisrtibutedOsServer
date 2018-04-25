/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miniprojectos;

import com.mysql.jdbc.Connection;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import static miniprojectos.TabletMP.conn;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author extra
 */
public class HandlingRequests implements Runnable {
    
    Queries q;
    Socket masterid;
    Socket clientid;
    String myDriver ;
    String myUrl ;
    JSONArray jsonArray = new JSONArray();
    JSONArray toMaster = new JSONArray();
    HandlingRequests( Queries q,Socket masterid)
    {
        this.q=q;
        this.clientid=q.id;
        this.masterid=masterid;
    }
    public  void DBConnection() throws ClassNotFoundException, SQLException
           {
             myDriver = "org.gjt.mm.mysql.Driver";
             myUrl = "jdbc:mysql://localhost/minip2os";
            Class.forName(myDriver);
            conn = (Connection) DriverManager.getConnection(myUrl, "root", "");

           }
     public  void Log()
           {
           
           }
    
    public  void parseMessage()
           {
           int tag=q.m.tag;
           if(tag==1)
           }
    public  void AddRow() throws ParseException, SQLException, IOException
           {
                JSONParser parser = new JSONParser();
                Object objj = parser.parse(q.m.a);
               JSONArray a = (JSONArray)objj;
            //  for(int i=0;i<a.size();i++)
              
                  JSONObject obj=(JSONObject)a.get(0);
               String id = (String)obj.get("rowKey");
                String title = (String)obj.get("title");
                String tags = (String)obj.get("tags");
                int views = (int)obj.get("views");
                int likes = (int)obj.get("likes");
                int dislikes= (int)obj.get("dislikes");
                int comment_count=(int)obj.get("comment_count");
                String description=(String)obj.get("description");
                String sql="INSERT INTO `table 1` (`rowKey`, `title`, `tags`, `views`, `likes`, `dislikes`, `comment_count`, `description`) VALUES\n" +
                    "('"+id+"', '"+title+"', '"+tags+"',"+views+" ,"+likes+","+dislikes+","+comment_count+", '"+description+"')";
                  
                  Statement st = conn.createStatement();
                  ResultSet rs = st.executeQuery(sql);
                  Message m=new Message() ;
                  m.tag=2;
                  ObjectOutputStream out = new ObjectOutputStream(q.id.getOutputStream());
                   out.writeObject(m);
                   out.flush();
                   
                    JSONObject item = new JSONObject();
             
             item.put("Query",sql );
             toMaster.add(item);
              
           
           }
    public  void DelRow()
           {
           
           }
    public  void ReadRows() throws ParseException, SQLException, IOException
           {
               JSONParser parser = new JSONParser();
                Object objj = parser.parse(q.m.a);
               JSONArray a = (JSONArray)objj;
               for(int i=0;i<a.size();i++)
             {   JSONObject obj=(JSONObject)a.get(i);
                  String id = (String)obj.get("rowKey");
                 String query = "SELECT * FROM `table 1` WHERE rowKey ='"+id+"'";
                 Statement st = conn.createStatement();
                  ResultSet rs = st.executeQuery(query);
                   
                   while (rs.next()) {
                       int total_rows = rs.getMetaData().getColumnCount();
                for (int j = 0; j < total_rows; j++) {
                        JSONObject objjj = new JSONObject();
                        objjj.put(rs.getMetaData().getColumnLabel(j + 1)
                        .toLowerCase(), rs.getObject(j + 1));
                      jsonArray.add(objjj);
                     }
            }
                  
             }
               Message m=new Message() ;
                  m.tag=3;
                  m.a=jsonArray.toString();
                  ObjectOutputStream out = new ObjectOutputStream(q.id.getOutputStream());
                   out.writeObject(m);
                   out.flush();
             
           }
     public  void Set() throws SQLException, ParseException
           {
             
           }
      public  void DelCells() throws ParseException, SQLException, IOException
           {
              JSONParser parser = new JSONParser();
                Object objj = parser.parse(q.m.a);
               JSONArray a = (JSONArray)objj;
                JSONObject obj=(JSONObject)a.get(0);
                String id = (String)obj.get("rowKey");
                for(int i=1;i<a.size();i++)
             { 
                 String idd = (String)obj.get("cellname");
              String sqlUpdate = "UPDATE `table 1` "
                + "SET '"+idd+"' = ? "
                + "WHERE rowKey = ?";
          
            PreparedStatement pstmt = conn.prepareStatement(sqlUpdate);
            pstmt.setString(1, null);
            pstmt.setString(2, id);
            int rowAffected = pstmt.executeUpdate();
             JSONObject item = new JSONObject();
             item.put("Query",sqlUpdate );
             toMaster.add(item);
             pstmt.close();
           
             }
                 
                 Message m=new Message() ;
                 m.tag=5;
                 ObjectOutputStream out = new ObjectOutputStream(q.id.getOutputStream());
                 out.writeObject(m);
                 out.flush();
           }
    @Override
    public void run()
    {
    
    }
    
}
