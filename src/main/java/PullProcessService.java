import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Date;


public class PullProcessService implements Runnable {


    DatabaseConnection conn;
    MongoDatabase db;
    public PullProcessService(){
        conn=new DatabaseConnection();
        db=conn.getDatabaseCreated();
    }

    public void run() {
        performOperation();
    }

    public void performOperation() {

        while(true){
            //Updating the Value of all the processes.

            try{
                //Decrementing Value of secToProcess by 1 ever second
                BasicDBObject newDoc = new BasicDBObject().append("$inc", new BasicDBObject().append("secToProcess", -1));
                BasicDBObject query = new BasicDBObject();
                db.getCollection("ProcessingList").updateMany(query, newDoc);


                //Removing processes where secToProcess value less than or euqal to 0

                BasicDBObject getLessZero=new BasicDBObject();
                getLessZero.put("secToProcess",new BasicDBObject("$lte",0));
                MongoCursor<Document> cursor=db.getCollection("ProcessingList").find(getLessZero).iterator();

                if(cursor!=null){

                    while(cursor.hasNext()){
                        Document processDocument=cursor.next();

                        //Deleting Process from the processlist
                        db.getCollection("ProcessingList").deleteOne(processDocument);

                        //Adding to TodoList after removing the secToProcess field
                        Date now=new Date();
                        processDocument.remove("secToProcess");
                        processDocument.append("insertedTime",now);
                        db.getCollection("ToDoList").insertOne(processDocument);
                    }
                }
                //This will hold the program for 1 second.
                Thread.sleep(1000);
            }

            catch (Exception e){
                System.out.println(e.getMessage());
            }




            //Iterating through all the documents and performing the insert operation on task


        }
    }
}
