import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Date;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class PullProcessBRR implements Runnable {


    DatabaseConnection conn;
    MongoDatabase db;
    public PullProcessBRR(){

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
                        db.getCollection("ProcessingList").deleteOne(processDocument);

                        //Variables for finding the random process time for the current documents
                        int currDocmin=(int)Math.round(db.getCollection("Customers").find(new BasicDBObject("_id",processDocument.get("customer_id").toString())).projection(fields(include("taskMinSeconds"))).first().getDouble("taskMinSeconds"));
                        int currDocmax=(int)Math.round(db.getCollection("Customers").find(new BasicDBObject("_id",processDocument.get("customer_id").toString())).projection(fields(include("taskMaxSeconds"))).first().getDouble("taskMaxSeconds"));
                        int range=(currDocmax-currDocmin)+1;

                        //Here we will get the task available for that particular customer
                        BasicDBObject whereCustomerid=new BasicDBObject();
                        whereCustomerid.put("customer_id",processDocument.get("customer_id"));

                        //Searching for the task in todoList
                        Document newDocument=db.getCollection("ToDoList").find(whereCustomerid).sort(new BasicDBObject("insertedTime",1)).first();

                        System.out.println("New Document"+newDocument);

                        //If the task associated with customer is available in the todolist then just adding the task to process list and inserting the removed task from processlist to todolist
                        if(newDocument!=null){
                            db.getCollection("ToDoList").deleteOne(newDocument);
                            Date now=new Date();
                            processDocument.remove("secToProcess");
                            processDocument.append("insertedTime",now);
                            db.getCollection("ToDoList").insertOne(processDocument);
                            newDocument.append("secToProcess",(int)(Math.random()*range)+currDocmin);
                            db.getCollection("ProcessingList").insertOne(newDocument);

                        }
                        else{

                            //As there were no more task for the particular customer adding the removed task from processlist back into process list with updated value
                            processDocument.put("secToProcess",(int)(Math.random()*range)+currDocmin);
                            db.getCollection("ProcessingList").insertOne(processDocument);
                        }


                    }
                }
                Thread.sleep(1000);
            }

            catch (Exception e){
                System.out.println(e.getMessage());
            }


        }
    }
}
