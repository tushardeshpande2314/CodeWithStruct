import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

public class DatabaseConnection {

   public static MongoClient connection;
    public static MongoDatabase database;

    public void establishConnection(){
        String hostname="localhost:27017";
        String dbname="TestMongo";
        String uri="mongodb://"+hostname;

        try{
            //Initalizing Connection and Database
            connection= MongoClients.create(uri);
            database=connection.getDatabase(dbname);
            System.out.println("Connected to Database");

        }
        catch(Exception e){
            // System.out.println(e.getMessage());
        }

    }

    public MongoDatabase getDatabaseCreated(){
        return database;
    }

    public MongoClient getClient(){
        return connection;
    }
}
