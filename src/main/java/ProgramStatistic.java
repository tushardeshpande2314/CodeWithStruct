import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Date;
import java.util.LinkedList;
import java.util.Queue;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class ProgramStatistic {

    DatabaseConnection conn;
    MongoDatabase db;
    public ProgramStatistic(){

        conn=new DatabaseConnection();
        db=conn.getDatabaseCreated();
    }

    public void showStatistic(){

        numberOfTaskByUser();
        totalProcessbyUser();
        totalTaskInProcess();
        totalTaskInTodo();

    }

    public void numberOfTaskByUser(){
        MongoCursor<Document> cursor=db.getCollection("Customers").find().iterator();
        Queue<Object> queue= new LinkedList<Object>();


        if(cursor!=null){
            while(cursor.hasNext()){
                queue.offer(cursor.next());
            }
        }
        System.out.println("Total Number of Task of Customer in Task list");
        //find the count number of task associated with each customer
        while (!queue.isEmpty()){

            Document doc=(Document) queue.poll();

            BasicDBObject selectTask=new BasicDBObject();
            selectTask.put("customer_id",doc.get("_id"));
            int count=(int)db.getCollection("ToDoList").countDocuments(selectTask);

            System.out.println(doc.get("_id")+": "+count);

            }
        }

        //This Method will provide Total Number of Processes by Customer
        public void totalProcessbyUser(){

            //Collecting customer data
            MongoCursor<Document> cursor=db.getCollection("Customers").find().iterator();
            Queue<Object> queue= new LinkedList<Object>();


            if(cursor!=null){
                while(cursor.hasNext()){
                    queue.offer(cursor.next());
                }
            }

            //Getting Total number of processes by user
            //find the count number of task associated with each customer

            System.out.println("Total Number of Task of Customer in Processing List");
            while (!queue.isEmpty()){

                Document doc=(Document) queue.poll();

                BasicDBObject selectTask=new BasicDBObject();
                selectTask.put("customer_id",doc.get("_id"));
                int count=(int)db.getCollection("ProcessingList").countDocuments(selectTask);

                System.out.println(doc.get("_id")+": "+count);

            }
        }

        //This Method Will Provide count of current task in task list
        public void totalTaskInTodo(){
            System.out.println("Total Number of Task in ToDoList"+(int)db.getCollection("ToDoList").countDocuments());
        }

        public void totalTaskInProcess(){
            System.out.println("Total Number of Task in ProcessList"+(int)db.getCollection("ProcessingList").countDocuments());
        }

        //This Method will print the current ProcessingList
        public void printingProcessingList(){

            MongoCursor cur=db.getCollection("ProcessingList").find().iterator();
            System.out.println("Current Processing List");

            while(cur.hasNext()){

                System.out.println(cur.next());
            }
        }

        //This Method will print current ToDoList
        public void printingToDoList(){
            MongoCursor cur=db.getCollection("ProcessingList").find().iterator();
            System.out.println("Current Processing List");
            while(cur.hasNext()){

                System.out.println(cur.next());
            }
        }
}
