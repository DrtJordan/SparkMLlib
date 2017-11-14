package dataPreprocessing;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import dbservice.MongoOperation;
import org.bson.Document;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class GetFeature {

    public void getFeatures(){

        int skip = 9074;

        MongoOperation userOperation = new MongoOperation();
        userOperation.setCollection("user");
        MongoCursor<Document> userCursor = userOperation.getDataSet(skip, 1500);

        MongoOperation postOperation = new MongoOperation();
        postOperation.setCollection("posts");
        MongoCollection<Document> postsCollection = postOperation.getCollection();

        try {
            File featureFile = new File("data\\feature.txt");
            featureFile.createNewFile();
            BufferedWriter bw = new BufferedWriter(new FileWriter(featureFile));

            int i = skip - 1;
            int flush = 0;

            while(userCursor.hasNext()){
                i += 1;
                flush += 1;
                Document d = userCursor.next();

                Document user = new Document();
                user.put("id", d.get("id"));
                user.put("name", d.get("name"));

                BasicDBObject selectCondition = new BasicDBObject();
                selectCondition.put("user", user);
                MongoCursor<Document> selectResult = postsCollection.find(selectCondition).iterator();

                int likeCount = 0;
                int faveCount = 0;
                int rewardCount = 0;

                while (selectResult.hasNext()){
//                    System.out.println(selectResult.next().get("post_id"));
                    Document post = selectResult.next();
                    int postLikeCount = (Integer)post.get("like_count");
                    int postFaveCount = (Integer)post.get("fav_count");
                    int postRewardCount = (Integer)post.get("reward_count");

                    likeCount += postLikeCount;
                    faveCount += postFaveCount;
                    rewardCount += postRewardCount;
                }

                boolean verified = (Boolean) d.get("verified");
                boolean verifiedRealName = (Boolean)d.get("verified_realname");
                int verif = verified ? 1 : 0;
                int verifRealName = verifiedRealName ? 1 : 0;
                int friendsCount = (Integer)d.get("friends_count");
                int followersCount = (Integer)d.get("followers_count");
                int stocksCount = (Integer)d.get("stocks_count");
                int postCount = (Integer)d.get("post_count");
                String line = verif + " " + verifRealName + " " +
                              friendsCount + " " + followersCount + " " +
                              stocksCount + " " + postCount + " " +
                              likeCount + " " + faveCount + " " +
                              rewardCount + "\r\n";
                System.out.println(i);
                System.out.println(line);

                bw.write(line);
                if (flush >= 100) {
                    bw.flush();
                    flush = 0;
                }
            }
            bw.flush();
            bw.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        GetFeature gf = new GetFeature();
        gf.getFeatures();
    }

}
