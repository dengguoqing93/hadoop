package job;

import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * POJO解决方案
 *
 * @author dengguoqing
 * @date 2019/3/2
 */
public class CommonFriends {
    public static Set<Integer> intersection(Set<Integer> user1friends,
                                            Set<Integer> user2friends) {
        if (CollectionUtils.isEmpty(user1friends) || CollectionUtils.isEmpty(
                user2friends)) {
            return null;
        }
        return user1friends.size() < user2friends.size() ?
                intersect(user1friends, user2friends) :
                intersect(user2friends, user1friends);

    }

    private static Set<Integer> intersect(Set<Integer> smallSet, Set<Integer> largeSet) {

        return smallSet.stream().filter(x -> largeSet.contains(x)).
                collect(Collectors.toSet());
    }
}
