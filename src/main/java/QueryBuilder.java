
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;

public class QueryBuilder {

    //How many Twitter users are in our database?
    public Integer question1(MongoDatabase db) {
        ArrayList<BsonValue> count = db.getCollection("tweets").distinct("user", BsonValue.class).
                filter(new Document("user", new Document("$ne", null))).
                into(new ArrayList<BsonValue>());
        return count.size();
    }

    //Which Twitter users link the most to other Twitter users? (Provide the top ten.)
    public void question2(MongoDatabase db) {
        MongoCollection<Document> collection = db.getCollection("tweets");
        AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
                new Document("$match", new Document("text", new Document("$regex", "/@\\\\S+/g"))),
                new Document("$sort", new Document("count", -1)),
                new Document("$limit", 10),
                new Document("$group", new Document("_id", "$user")
                        .append("count", new Document("$sum", 1)))
        ));
        for (Document dbObject : output) {
            System.out.println(dbObject.get("_id"));
        }
    }

    //Who is are the most mentioned Twitter users? (Provide the top five.) Not finished.
    public void question3(MongoDatabase db) {
        MongoCollection<Document> collection = db.getCollection("tweets");
        AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
                new Document("$match", new Document("$regex", new Document("@\\\\w+", "ig"))),
                new Document("$group", new Document("_id", "$user")
                        .append("tweets", new Document("$sum", 1))),
                new Document("$sort", new Document("tweets", -1)),
                new Document("$limit", 5)
        )).allowDiskUse(true);
        for (Document dbObject : output) {
            System.out.println(dbObject.get("_id"));
        }
    }

    //Who are the most active Twitter users (top ten)?
    public void question4(MongoDatabase db) {
        MongoCollection<Document> collection = db.getCollection("tweets");
        AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", new Document("user", "$user")
                        .append("tweetId", "$id"))),
                new Document("$group", new Document("_id", "$_id.user")
                        .append("count", new Document("$sum", 1))),
                new Document("$sort", new Document("count", -1)),
                new Document("$limit", 10)
        )).allowDiskUse(true);
        for (Document dbObject : output) {
            System.out.println(dbObject.get("_id"));
        }

    }

    ;

    //Who are the five most grumpy (most negative tweets)?
    public void question5(MongoDatabase db) {
        MongoCollection<Document> collection = db.getCollection("tweets");
        AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$user")
                        .append("average_polarity", new Document("$avg", "$polarity"))),
                new Document("$sort", new Document("average_polarity", 1)),
                new Document("$limit", 5)
        )).allowDiskUse(true);

        for (Document dbObject : output) {
            System.out.println(dbObject.get("_id"));
        }
    }

    //Most happy?
    public void question6(MongoDatabase db) {
        MongoCollection<Document> collection = db.getCollection("tweets");
        AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
                new Document("$group", new Document("_id", "$user")
                        .append("average_polarity", new Document("$avg", "$polarity"))),
                new Document("$sort", new Document("average_polarity", -1)),
                new Document("$limit", 5)
        )).allowDiskUse(true);
//        return "db.tweets.aggregate( [" +
//                "    {     $group: {       _id: '$user'," +
//                "       average_polarity: { $avg: '$polarity' }," +
//                "     }," +
//                "   }, " +
//                "  {     $sort: { average_polarity: -1 }," +
//                "   }," +
//                "   {     $limit: 5,   }" +
//                ",]" +
//                ",{ allowDiskUse: true })";
        for (Document dbObject : output) {
            System.out.println(dbObject.get("_id"));
        }
    }

}
