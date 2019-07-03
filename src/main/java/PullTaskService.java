import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class PullTaskService implements Runnable {


    DatabaseConnection conn;
    MongoDatabase db;
    int MAX_TASK_PROCESSING=100;
    public PullTaskService(int maxTaskProcessing){
        conn=new DatabaseConnection();
        db=conn.getDatabaseCreated();
        this.MAX_TASK_PROCESSING=maxTaskProcessing;
    }
    public void run() {
        //db.getCollection("ToDoList").
        perfromOperation();
    }

    public void perfromOperation(){
        while(true) {
            try{

                //Selecting items from the TodoList sorted by insertedTime
                Document doc = db.getCollection("ToDoList").find().sort(new BasicDBObject("insertedTime", 1)).first();
                //int min;
                //int max;
                //int range;


                //Selecting customer id from the TodoList and taking out customer min and max seconds from Customers
                BasicDBObject field = new BasicDBObject();
                field.put("_id", doc.get("customer_id").toString());

                //Old change had query in the find()  parameter
                double min = Math.round(db.getCollection("Customers").find(field).projection(fields(include("taskMinSeconds"))).first().getDouble("taskMinSeconds"));
                double max = Math.round(db.getCollection("Customers").find(field).projection(fields(include("taskMaxSeconds"))).first().getDouble("taskMaxSeconds"));
                double range = Math.round((max - min) + 1);



                double time =Math.round((Math.random()*range)+min);

                //Adding the document into the Processing list

                if (db.getCollection("ProcessingList").countDocuments() < MAX_TASK_PROCESSING) {
                    //Removing the document from the ToDoList

                    System.out.println("Deleting Document from "+doc);
                    db.getCollection("ToDoList").deleteOne(doc);

                    //Adding the Extra field into the collection to determine the amount the time process spend in processing list
                    doc.append("secToProcess", time);
                    System.out.println("Inserting Document to ProcessingList:"+doc);
                    db.getCollection("ProcessingList").insertOne(doc);
                }

                Thread.sleep(1000);

            }catch(Exception e){
                System.out.println(e.getMessage());
            }

            //System.out.println(min+" "+max+" "+range+" "+range);
        }

    }
}
