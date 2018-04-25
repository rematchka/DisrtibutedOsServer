/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package miniprojectos;

import com.mysql.jdbc.Connection;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;




public class TabletMP {

    /**
     * @param args the command line arguments
     */
    static Socket SMid;
    static Socket scid;
    static Connection conn;
    static ServerSocket listenerClient=null;
    static ServerSocket listenerMaster=null;
    static String range="";
    static String rowid1,rowid2,rowid3,rowid4; 
    static HashMap<String,Integer> LockedRows =   new HashMap<String,Integer>();
    static List<Queries>PendingQueries=new ArrayList<Queries>();
    static List<Queries>PendingQueriestoStart=new ArrayList<Queries>();
    static int cntNumberOfOperation=0;
    static List<ThreadsTracked>treadsToCheck=new  ArrayList<ThreadsTracked>();
    static Set <String>updatedRows=new HashSet<String>();
   
               

    /////////////////need to think of ranges////////////////////////////////////////////
    /////////////////need to keep track of updates///////////////////////////////////////////
    
 public static void initTablet()
    {

        try
        {
            // create our mysql database connection
            String myDriver = "org.gjt.mm.mysql.Driver";
            String myUrl = "jdbc:mysql://localhost/minip2os";
            Class.forName(myDriver);
            conn = (Connection) DriverManager.getConnection(myUrl, "root", "");

            // our SQL SELECT query.
            // if you only need a few columns, specify them by name instead of using "*"
     /*       String query = "SELECT * FROM `table 1` ";

            // create the java statement
            Statement st = conn.createStatement();

            // execute the query, and get a java resultset
            ResultSet rs = st.executeQuery(query);

            // iterate through the java resultset
            while (rs.next())
            {
                String id = rs.getString("rowKey");
                String firstName = rs.getString("title");


                // print the results
                System.out.format("%s\n", id, firstName);
            }
            st.close();*/
        }
        catch (Exception e)
        {
            System.err.println("Got an exception! ");
            System.err.println(e.getMessage());
        }




    }
        
        
        public static void openConnection() throws UnknownHostException, IOException, ClassNotFoundException, SQLException, ParseException
        {    
            listenerMaster = new ServerSocket(9000);
            SMid=listenerMaster.accept();
           // SendIP();
           initTablet();
            LoadDB();
             
             
        }
        
        public static void SendIP() throws IOException
        {
            ObjectOutputStream out = new ObjectOutputStream(SMid.getOutputStream());
             Message m=new Message();
             m.tag=1;
             JSONObject item = new JSONObject();
             String ip="";
             item.put("ip", ip);
             m.a=ip;
             out.writeObject(m);
             out.flush();
            
        }
        public static void LoadDB() throws UnknownHostException, IOException, ClassNotFoundException, SQLException, ParseException
          {
             //SMid=listenerMaster.accept();
             ObjectInputStream is = new ObjectInputStream(SMid.getInputStream());
             Message m=(Message) is.readObject();
              if(m.tag==1)
              {  
                  range=m.mid;
                  StoreDB(m);
               
               }
          }
        public static void StoreDB(Message m) throws UnknownHostException, IOException, ClassNotFoundException, SQLException, ParseException
          {
                 JSONParser parser = new JSONParser();
                Object objj = parser.parse(m.a);
               JSONArray a = (JSONArray)objj;
              for(int i=0;i<a.size();i++)
              {
                JSONObject obj=(JSONObject)a.get(i);
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
                  
                  if(i==0)rowid1=id;
                  if(i==a.size()-1/2)rowid2=id;
                  if(i==a.size()/2)rowid3=id;
                  if(i==a.size()-1)rowid4=id;
              }
           //range to be changes
          }
          
          
        public static void openConnectionClient() throws UnknownHostException, IOException, ClassNotFoundException, ParseException
           {      
                listenerClient = new ServerSocket(8800);
                
           
                    while(true)
                     {
                         
                         checkPending();
                         checkPendingToSartFast();
                         checkThread();
                         //checkToUpdateMaster();
                              
                         
                         scid=listenerClient.accept(); 
                         ObjectOutputStream out = new ObjectOutputStream(scid.getOutputStream());
                         ObjectInputStream is = new ObjectInputStream(scid.getInputStream());
                         Message m=(Message) is.readObject();
                         if(m==null)continue;
                         Queries q=new Queries();
                         q.id=scid;
                         q.m=m;
                         
                         //////////check on locking condition////////////
                         if(m.tag==4 ||m.tag==5||m.tag==6) 
                         {///////////////////not sure there will not be deadlock///////////////////////////////
                            boolean lock= parseMessage(m);
                             if(lock)
                             {
                                 PendingQueries.add(q);
                                 SendBlock( scid);
                             }
                             else
                              ////////in case running////////////////////
                             {  Thread t1 = new Thread(new HandlingRequests(q,SMid));
                                t1.start();
                                ThreadsTracked tt=new ThreadsTracked();
                                tt.m=m;
                                tt.t=t1;
                                treadsToCheck.add(tt);
                               
                                 
                             }
                         }
                         else {
                            boolean lockrow= parseMessage2(m);
                            if(lockrow)
                            {
                                PendingQueriestoStart.add(q);
                                 SendBlock( scid);
                            }
                             else
                            {
                                Thread t1 = new Thread(new HandlingRequests(q,SMid));
                                 t1.start(); 
                            }
                             
                         }

                     }
            
        }
        public static void  SendBlock( Socket id) throws IOException
           {  ObjectOutputStream out = new ObjectOutputStream(id.getOutputStream());
             Message m=new Message();
             m.tag=7;
             JSONObject item = new JSONObject();
             
             item.put("Bock", "YES");
             JSONArray jA=new JSONArray();
             jA.add(item);
             m.a=jA.toString();
             out.writeObject(m);
             out.flush();
           }
          public static void  SendUNBlock( Socket id) throws IOException
           {  ObjectOutputStream out = new ObjectOutputStream(id.getOutputStream());
             Message m=new Message();
             m.tag=7;
             JSONObject item = new JSONObject();
             
             item.put("Bock", "NO");
             JSONArray jA=new JSONArray();
             jA.add(item);
             m.a=jA.toString();
             out.writeObject(m);
             out.flush();
           }
        public static boolean parseMessage(Message m) throws ParseException
           {
                JSONParser parser = new JSONParser();
             Object objj = parser.parse(m.a);
             JSONArray a = (JSONArray)objj;
              for(int i=0;i<a.size();i++)
              {
                JSONObject obj=(JSONObject)a.get(i);
                String id = (String)obj.get("rowKey");
                if(LockedRows.get(id)==1)
                {
                    
                    
                     return true;
                }
                else 
                {    updatedRows.add(id);
                    LockedRows.put(id, 1);
                }
              }
              return false;
           }
         public static boolean parseMessage2(Message m) throws ParseException
         {
             JSONParser parser = new JSONParser();
             Object objj = parser.parse(m.a);
             JSONArray a = (JSONArray)objj;
              for(int i=0;i<a.size();i++)
              {
                JSONObject obj=(JSONObject)a.get(i);
                String id = (String)obj.get("rowKey");
                if(LockedRows.get(id)==1)
                {
                    
                    
                     return true;
                }
                
              }
              return false;
         }
        public static void checkPending() throws IOException, ParseException
           {
               ///if pending but row unlocked send un block and create thread
               
               
              for(int i=0;i<PendingQueries.size();i++)
            {
                if(i<0)
                   i=0;
                JSONParser parser = new JSONParser();
                Object objj = parser.parse(PendingQueries.get(i).m.a);
                JSONArray jA = (JSONArray)objj;
                
               // JSONArray jA=PendingQueries.get(i).m.a;
                boolean lock=true;
              //  for(int j=0;j<jA.size();j++)
                
                    JSONObject jO=(JSONObject)jA.get(0);
                    String rowID=(String)jO.get("rowKey");
                    if(LockedRows.get(rowID)==1)
                    {
                        lock=false;
                        //break;
                    }
                    
                  //if(jA.get(j).get("rowKey"))
                
                if(lock)
                {
                     Thread t1 = new Thread(new HandlingRequests(PendingQueries.get(i),SMid));
                     t1.start(); 
                     SendUNBlock(PendingQueries.get(i).id);
                     updatedRows.add(rowID);
                    LockedRows.put(rowID, 1);
                     ThreadsTracked tt=new ThreadsTracked();
                     tt.m=PendingQueries.get(i).m;
                     tt.t=t1;
                    treadsToCheck.add(tt);
                     PendingQueries.remove(i);
                     i-=2;
                     
                }
            }
               
           }
        public static void checkPendingToSartFast() throws IOException, ParseException
        {
            for(int i=0;i<PendingQueriestoStart.size();i++)
            {
                 if(i<0)
                   i=0;
                JSONParser parser = new JSONParser();
                Object objj = parser.parse(PendingQueriestoStart.get(i).m.a);
                JSONArray jA = (JSONArray)objj;
                
                boolean lock=true;
                for(int j=0;j<jA.size();j++)
                {
                    JSONObject jO=(JSONObject)jA.get(j);
                    String rowID=(String)jO.get("rowKey");
                    if(LockedRows.get(rowID)==1)
                    {
                        lock=false;
                        break;
                    }
                    
                  //if(jA.get(j).get("rowKey"))
                }
                if(lock)
                {
                     Thread t1 = new Thread(new HandlingRequests(PendingQueriestoStart.get(i),SMid));
                     t1.start(); 
                     SendUNBlock(PendingQueriestoStart.get(i).id);
                     PendingQueriestoStart.remove(i);
                     i-=2;
                     
                }
            }
        }
         public static void  checkThread() throws ParseException
          {
                ///////////////////////////
             /////
             /*
             
             
             displayStateAndIsAlive(thread);
		Thread.sleep(1000);
		displayStateAndIsAlive(thread);
             if (t1.isAlive()) {
            System.out.format("%s is alive.%n", t1.getName());
        } else {
            System.out.format("%s is not alive.%n", t1.getName());
        }
             */
              for(int i=0;i<treadsToCheck.size();i++)
              {
                   if(i<0)
                   i=0;
                  Message m= treadsToCheck.get(i).m;
                  
                  JSONParser parser = new JSONParser();
                  Object objj = parser.parse(m.a);
                  JSONArray a = (JSONArray)objj;
                  JSONObject obj=(JSONObject)a.get(0);
                  String id = (String)obj.get("rowKey");
                  
                  if (!treadsToCheck.get(i).t.isAlive())
                   {LockedRows.put(id,0);
                   treadsToCheck.remove(i);
                   i-=2;
                   }
              }
              
             
              
           }
         public static void checkToUpdateMaster()
         {
            
         }
    public static void main(String[] args) throws IOException, UnknownHostException, ClassNotFoundException, SQLException, ParseException {
        openConnection();
        openConnectionClient();
        
       // LoadDB();
        
        
    }
}
