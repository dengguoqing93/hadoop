package job;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * 共同好友归约器
 *
 * @author dengguoqing
 * @date 2019/3/2
 */
public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {


    /**
     * The goal is to find common friends by intersecting all lists defined in values parameter.
     *
     * @param key    is a pair: <user_id_1><,><user_id_2>
     * @param values is a list of { <friend_1><,>...<,><friend_n> }
     */
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<>();
        Iterator<Text> iterator = values.iterator();
        int numOfValues = 0;
        while (iterator.hasNext()) {
            String friends = iterator.next().toString();
            if (StringUtils.equals(friends, "")) {
                context.write(key, new Text("[]"));
                return;
            }
            addFriends(map, friends);
            numOfValues++;
        }

        // now iterate the map to see how many have numOfValues
        List<String> commonFriends = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() == numOfValues) {
                commonFriends.add(entry.getKey());
            }
        }

        context.write(key, new Text(commonFriends.toString()));
    }

    static void addFriends(Map<String, Integer> map, String friendsList) {
        String[] friends = StringUtils.split(friendsList, ",");
        for (String friend : friends) {
            Integer count = map.get(friend);
            if (count == null) {
                map.put(friend, 1);
            } else {
                map.put(friend, ++count);
            }
        }
    }
}
