import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.swing.text.html.parser.DocumentParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.*;

public class MainThreadClass {
    public static void main(String args[]){

        DatabaseConnection conn=new DatabaseConnection();
        conn.establishConnection();
        MongoDatabase db=conn.getDatabaseCreated();

        ProgramStatistic programStats=new ProgramStatistic();
        System.out.println("Connection Established");
        int minPushTask;
        int choice=Integer.MAX_VALUE;
        Scanner sc=new Scanner(System.in);
        int MAX_TASK_PROCESSING=100;
        //Initialized Threads to run all the processes
        Thread todo;
        Thread process;
        int stop;

        System.out.println("Please Enter MAX_TASK_PROCESSING Number");
        MAX_TASK_PROCESSING=sc.nextInt();

        //Creating Menu to Perform the Task
        while(choice!=9){
            System.out.println("Please Select Options from below");
            System.out.println("1) Load Data");
            System.out.println("2) First In First Out Algorithm");
            System.out.println("3) Round Robin Algorithm");
            System.out.println("4) Balanced Round Robin");
            System.out.println("5) Print Statistic");
            System.out.println("6) Print Current ProcessingList");
            System.out.println("7) Print Current TodoList");
            System.out.println("8) Reset");
            System.out.println("9) Exit");
            choice=sc.nextInt();

            switch(choice){
                case 1:
                    System.out.println("Loading Data");
                    try{
                       // Document doc=Document.parse(readData());
                        db.getCollection("ToDoList").drop();
                        db.getCollection("ProcessingList").drop();
                        db.getCollection("Customers").insertMany(readData("customer.txt"));
                        db.getCollection("ToDoList").insertMany(readData("data.txt"));


                    }catch(Exception e){
                        System.out.println(e.getMessage());
                    }

                    break;
                case 2:
                    System.out.println("Starting First In First Out");

                        System.out.println("Run Thread");
                        todo=new Thread(new PullTaskService(MAX_TASK_PROCESSING));
                        process=new Thread(new PullProcessService());

                        todo.start();
                        process.start();

                        //This will keep the thread running and when we enter a number thread will stop
                        stop=sc.nextInt();

                        System.out.println("Thread Stopped");
                        todo.stop();
                        process.stop();

                    break;
                case 3:
                    System.out.println("Starting Round Robin Algorithm");
                        todo=new Thread(new PullTodoRR(MAX_TASK_PROCESSING));
                        process=new Thread(new PullProcessService());

                        todo.start();
                        process.start();

                        stop=sc.nextInt();
                        System.out.println("Thread Stopped");
                        todo.stop();
                        process.stop();
                    break;
                case 4:
                    System.out.println("Starting Balanced Round Robin");
                        minPushTask=returnMintask();
                        todo=new Thread(new BalancedRoundRobin(minPushTask,MAX_TASK_PROCESSING));
                        process=new Thread(new PullProcessBRR());

                        todo.start();
                        process.start();

                        stop=sc.nextInt();
                        System.out.println("Thread Stopped");
                        todo.stop();
                        process.stop();

                    break;
                case 5:
                    System.out.println("Printing Statistics of the System");
                        programStats.showStatistic();
                    break;
                case 6:
                    System.out.println("Printing Current ProcessingList");
                        programStats.printingProcessingList();
                    break;
                case 7:
                    System.out.println("Printing Current TodoList");
                        programStats.printingToDoList();
                    break;
                case 8:
                    System.out.println("Reseting the System");

                    Thread.currentThread().interrupt();
                    break;
                case 9:
                    System.out.println("Exiting System");
                    System.exit(0);
                    break;
                    default:
                        System.out.println("Please Choose valid option");
                        break;
            }
        }

    }


    public static List<Document> readData(String fileName) throws Exception{
        String path="C:\\Users\\tusha\\Documents\\demo.clar\\FIFOThreads\\src\\main\\java";
        File file = new File("C:\\Users\\tusha\\Documents\\demo.clar\\FIFOThreads\\src\\main\\java\\"+fileName);

        BufferedReader br = new BufferedReader(new FileReader(file));

        String st;
        List<Document> list=new ArrayList<Document>();
        while ((st = br.readLine()) != null){
            System.out.println(st);
            list.add(Document.parse(st));
        }


        return list;
    }

    public static int returnMintask(){

        DatabaseConnection conn=new DatabaseConnection();
        MongoDatabase db=conn.getDatabaseCreated();
        int minCount=Integer.MAX_VALUE;

        //find the total customer that are there in the list
        MongoCursor<Document> cursor=db.getCollection("Customers").find().iterator();
        Queue<Object> queue= new LinkedList<Object>();


        if(cursor!=null){
            while(cursor.hasNext()){
                queue.offer(cursor.next());
            }
        }
        //find the count number of task associated with each customer
        while (!queue.isEmpty()){

            Document doc=(Document) queue.poll();

            BasicDBObject selectTask=new BasicDBObject();
            selectTask.put("customer_id",doc.get("_id"));
            int count=(int)db.getCollection("ToDoList").countDocuments(selectTask);

            //System.out.println(count);
            if(count<minCount){
                minCount=count;
            }
        }
        //return the minimum count
        return minCount;
    }
}
