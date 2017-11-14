package dbservice;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoOperation {

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;

    public MongoOperation(){
        this.mongoClient = new MongoClient("localhost", 27017);
        System.out.println("Connect to database successfully");
        // 连接到数据库
        this.mongoDatabase = this.mongoClient.getDatabase("test");
        System.out.println("connect to database success");
    }

    public void setCollection(String collection){
        this.mongoCollection = this.mongoDatabase.getCollection(collection);
        System.out.printf("collection %s select success \n", collection);
    }

    public MongoCollection<Document> getCollection(String collection){
        if(this.mongoCollection == null){
            this.mongoCollection = this.mongoDatabase.getCollection(collection);
            System.out.printf("collection %s select success \n", collection);
        }
        return this.mongoCollection;
    }

    public MongoCollection<Document> getCollection(){
        if (this.mongoCollection == null) {
            System.out.println("please set collection first");
            return null;
        }
        return this.mongoCollection;
    }

    public MongoCursor<Document> getDataSet(){
        return this.mongoCollection.find().iterator();
    }

    public MongoCursor<Document> getDataSet(int skip, int limit){
        return this.mongoCollection.find().skip(skip).limit(limit).iterator();
    }

    public static void main(String[] args) {
        MongoOperation mongoOperation = new MongoOperation();
        mongoOperation.setCollection("post_analysis");
        MongoCursor<Document> mongoCursor = mongoOperation.getDataSet();
        while (mongoCursor.hasNext()) {
            Document d = mongoCursor.next();
            System.out.println(d.get("created_at") + "   " + d.get("post_text"));
        }
    }

}
