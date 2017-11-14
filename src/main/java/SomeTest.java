import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DBCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import dbservice.MongoOperation;
import org.bson.Document;

public class SomeTest {

    public static void main(String[] args) {
        MongoOperation mo = new MongoOperation();
//        mo.setCollection("posts");
        MongoCollection<Document> postsCollection = mo.getCollection("posts");

//        BasicDBObject select = new BasicDBObject("user", new BasicDBObject("id", "3892760630"));
        BasicDBObject select = new BasicDBObject();
        Document user = new Document();
        user.put("id", Long.parseLong("1670274607"));
        user.put("name", "跟着节奏摇");
        select.put("user", user);

        FindIterable<Document> result = postsCollection.find(select);

        MongoCursor<Document> selectResult = result.iterator();

        System.out.println("select over");

        int i = 0;

        while (selectResult.hasNext()){
            System.out.println(selectResult.next().get("post_id"));
//            System.out.println(i);
//            i += 1;
        }


    }

}
