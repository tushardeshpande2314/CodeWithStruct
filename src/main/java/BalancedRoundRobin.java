import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.LinkedList;
import java.util.Queue;

public class BalancedRoundRobin implements Runnable {

    int minTask;
    DatabaseConnection conn;
    MongoDatabase db;

    int MAX_TASK_PROCESS=100;

    public void run() {
        performOperation();
    }

    public BalancedRoundRobin(int minTaskPush,int maxTaskProcess){
        this.minTask=minTaskPush;
        conn=new DatabaseConnection();
        db=conn.getDatabaseCreated();
        MAX_TASK_PROCESS=maxTaskProcess;
    }

    public void performOperation(){
        MongoCursor<Document> cursor=db.getCollection("Customers").find().iterator();
        Queue<Object> queue= new LinkedList<Object>();
        //projection(fields(include("_id","")))





        if(cursor!=null){
            while(cursor.hasNext()){
                queue.offer(cursor.next());
            }
        }

        //Calculating Number of Process to push by considering the size of queue and minTask

        int processToPush=minTask*queue.size();

        //Process to push is Minimum than
        if(processToPush>MAX_TASK_PROCESS){

            int processPerCustomer=MAX_TASK_PROCESS/queue.size();
            processToPush=queue.size()*processPerCustomer;

        }




        //Pushing the Process in Round Robin Fashion in the Processing List
        for(int i=0;i<processToPush;i++){
            try{
                //This process is to take out the value of customer in the queue
                Document dot=(Document)queue.poll();
                int min=(int)Math.round(dot.getDouble("taskMinSeconds"));
                int max=(int)Math.round(dot.getDouble("taskMaxSeconds"));
                int range=(max-min)+1;
                int time=(int)Math.round((Math.random()*range)+min);

                //System.out.println(min+" "+max+" "+time);
                //BasicDBObject query=new BasicDBObject();
                BasicDBObject customerId=new BasicDBObject();
                customerId.put("customer_id",dot.get("_id"));

                //Selecting the Document to Move
                Document doc=db.getCollection("ToDoList").find(customerId).sort(new BasicDBObject("insertedTime",1)).first();


                //Removing the document from the ToDoList
                System.out.println("Deleting Document from "+doc);
                db.getCollection("ToDoList").deleteOne(doc);

                //Adding the Extra field into the collection to determine the
                doc.append("secToProcess", time);
                System.out.println("Inserting Document to ProcessingList:"+doc);

                //Adding the task into processing list
                db.getCollection("ProcessingList").insertOne(doc);
                System.out.println(doc);
                queue.offer(dot);
        //        Thread.sleep(1000);

            }catch(Exception e){
                System.out.println(e.getMessage());
            }
        }

    }
}
