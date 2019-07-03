import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;


import javax.print.Doc;
import java.util.LinkedList;
import java.util.Queue;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class PullTodoRR implements Runnable {

    DatabaseConnection conn;
    MongoDatabase db;
    int MAX_TASK_PROCESSING=100;
    public PullTodoRR(int maxTaskProcessing){

        conn=new DatabaseConnection();
        db=conn.getDatabaseCreated();
        this.MAX_TASK_PROCESSING=maxTaskProcessing;
    }

    public void run() {
        performOperation();
    }

    public void performOperation(){

        //Selecting all the customers from the customers and adding them in queue
        MongoCursor<Document> cursor=db.getCollection("Customers").find().iterator();
        Queue<Object> queue= new LinkedList<Object>();
        //projection(fields(include("_id","")))





        //Adding customers into queue
        if(cursor!=null){
            while(cursor.hasNext()){
                queue.offer(cursor.next());
            }
        }

       /* while(!queue.isEmpty()){
            Document dot=(Document) queue.poll();
            System.out.println(dot.get("_id"));
            System.out.println(dot.getDouble("taskMaxSeconds"));
            System.out.println(dot.getDouble("taskMinSeconds"));
            //System.out.println(queue.poll());

        }*/

       while(true){
            try{

                //This process is to take out the value of customer in the queue
                Document dot=(Document)queue.poll();
                int min=(int)Math.round(dot.getDouble("taskMinSeconds"));
                int max=(int)Math.round(dot.getDouble("taskMaxSeconds"));
                int range=(max-min)+1;
                int time=(int)Math.round((Math.random()*range)+min);

                //System.out.println(min+" "+max+" "+time);
                //BasicDBObject query=new BasicDBObject();

                //Selecting earliest task from TODOList based on the customer in round robin fashion
                BasicDBObject customerId=new BasicDBObject();
                customerId.put("customer_id",dot.get("_id"));

                Document doc=db.getCollection("ToDoList").find(customerId).sort(new BasicDBObject("insertedTime",1)).first();

                if(doc!=null && db.getCollection("ProcessingList").countDocuments() < MAX_TASK_PROCESSING){
                    //Removing the document from the ToDoList

                    System.out.println("Deleting Document from "+doc);
                    db.getCollection("ToDoList").deleteOne(doc);

                    //Adding the Extra field into the collection to determine the time the process should stay in processing list
                    doc.append("secToProcess", time);
                    System.out.println("Inserting Document to ProcessingList:"+doc);
                    db.getCollection("ProcessingList").insertOne(doc);

                }

                System.out.println(doc);
                queue.offer(dot);
                Thread.sleep(1000);

            }catch(Exception e){

            }
        }
    }
}
