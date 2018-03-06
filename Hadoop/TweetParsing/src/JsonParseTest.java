import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.HashSet;

public class JsonParseTest {
    public static void main(String[] args) {
        JSONParser parser = new JSONParser();

        try {
            JSONObject jsonObj = (JSONObject) parser.parse("{\"metadata\":{\"result_type\":\"recent\",\"iso_language_code\":\"en\"},\"in_reply_to_status_id_str\":null,\"in_reply_to_status_id\":null,\"created_at\":\"Fri Feb 20 18:46:35 +0000 2015\",\"in_reply_to_user_id_str\":null,\"source\":\"<a href=\\\"http:\\/\\/twitter.com\\/download\\/iphone\\\" rel=\\\"nofollow\\\">Twitter for iPhone<\\/a>\",\"retweeted_status\":{\"metadata\":{\"result_type\":\"recent\",\"iso_language_code\":\"en\"},\"in_reply_to_status_id_str\":null,\"in_reply_to_status_id\":null,\"possibly_sensitive\":false,\"coordinates\":null,\"created_at\":\"Fri Feb 20 18:33:44 +0000 2015\",\"in_reply_to_user_id_str\":null,\"truncated\":false,\"source\":\"<a href=\\\"http:\\/\\/twitter.com\\\" rel=\\\"nofollow\\\">Twitter Web Client<\\/a>\",\"retweet_count\":1,\"retweeted\":false,\"geo\":null,\"in_reply_to_screen_name\":null,\"entities\":{\"urls\":[{\"display_url\":\"thebaselinemagazine.com\",\"indices\":[95,117],\"expanded_url\":\"http:\\/\\/thebaselinemagazine.com\",\"url\":\"http:\\/\\/t.co\\/dNwB4FHehy\"}],\"hashtags\":[{\"indices\":[13,20],\"text\":\"Oscars\"},{\"indices\":[50,65],\"text\":\"AmericanSniper\"}],\"media\":[{\"display_url\":\"pic.twitter.com\\/AQTq6tW23G\",\"indices\":[118,140],\"sizes\":{\"small\":{\"w\":340,\"h\":173,\"resize\":\"fit\"},\"large\":{\"w\":591,\"h\":301,\"resize\":\"fit\"},\"thumb\":{\"w\":150,\"h\":150,\"resize\":\"crop\"},\"medium\":{\"w\":591,\"h\":301,\"resize\":\"fit\"}},\"id_str\":\"568840945341181952\",\"expanded_url\":\"http:\\/\\/twitter.com\\/thebaselinemag\\/status\\/568840945791168512\\/photo\\/1\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/B-Tt03DCMAALGpK.jpg\",\"id\":568840945341181952,\"type\":\"photo\",\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/B-Tt03DCMAALGpK.jpg\",\"url\":\"http:\\/\\/t.co\\/AQTq6tW23G\"}],\"user_mentions\":[],\"symbols\":[]},\"id_str\":\"568840945791168512\",\"in_reply_to_user_id\":null,\"favorite_count\":0,\"contributors\":null,\"id\":568840945791168512,\"place\":null,\"text\":\"Countdown to #Oscars!  Will  Hanajun 's review of #AmericanSniper hold true?!  Check it out at http:\\/\\/t.co\\/dNwB4FHehy http:\\/\\/t.co\\/AQTq6tW23G\",\"lang\":\"en\",\"user\":{\"utc_offset\":null,\"friends_count\":36,\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/3540885842\\/e25b284069ede181b4801a40913c1aed_normal.jpeg\",\"listed_count\":0,\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"default_profile_image\":false,\"favourites_count\":145,\"created_at\":\"Thu Apr 18 22:16:48 +0000 2013\",\"description\":\"\",\"is_translator\":false,\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"protected\":false,\"screen_name\":\"thebaselinemag\",\"id_str\":\"1363006170\",\"profile_link_color\":\"0084B4\",\"is_translation_enabled\":false,\"geo_enabled\":false,\"id\":1363006170,\"profile_background_color\":\"C0DEED\",\"lang\":\"en\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_location\":null,\"profile_text_color\":\"333333\",\"verified\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/3540885842\\/e25b284069ede181b4801a40913c1aed_normal.jpeg\",\"time_zone\":null,\"contributors_enabled\":false,\"url\":\"http:\\/\\/t.co\\/OxiN2XQwS9\",\"profile_background_tile\":false,\"entities\":{\"description\":{\"urls\":[]},\"url\":{\"urls\":[{\"display_url\":\"thebaselinemagazine.com\",\"indices\":[0,22],\"expanded_url\":\"http:\\/\\/www.thebaselinemagazine.com\",\"url\":\"http:\\/\\/t.co\\/OxiN2XQwS9\"}]}},\"follow_request_sent\":false,\"statuses_count\":203,\"default_profile\":true,\"followers_count\":52,\"following\":false,\"profile_use_background_image\":true,\"name\":\" the base line \",\"location\":\"\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"notifications\":false},\"favorited\":false},\"retweet_count\":1,\"retweeted\":false,\"geo\":null,\"in_reply_to_screen_name\":null,\"id_str\":\"568844180836913152\",\"in_reply_to_user_id\":null,\"favorite_count\":0,\"id\":568844180836913152,\"place\":null,\"text\":\"RT @thebaselinemag: Countdown to #Oscars!  Will  Hanajun 's review of #AmericanSniper hold true?!  Check it out at http:\\/\\/t.co\\/dNwB4FHehy h\\u2026\",\"lang\":\"en\",\"favorited\":false,\"possibly_sensitive\":false,\"coordinates\":null,\"truncated\":false,\"entities\":{\"urls\":[{\"display_url\":\"thebaselinemagazine.com\",\"indices\":[115,137],\"expanded_url\":\"http:\\/\\/thebaselinemagazine.com\",\"url\":\"http:\\/\\/t.co\\/dNwB4FHehy\"}],\"hashtags\":[{\"indices\":[33,40],\"text\":\"Oscars\"},{\"indices\":[70,85],\"text\":\"AmericanSniper\"}],\"media\":[{\"display_url\":\"pic.twitter.com\\/AQTq6tW23G\",\"source_user_id\":1363006170,\"type\":\"photo\",\"media_url\":\"http:\\/\\/pbs.twimg.com\\/media\\/B-Tt03DCMAALGpK.jpg\",\"source_status_id\":568840945791168512,\"url\":\"http:\\/\\/t.co\\/AQTq6tW23G\",\"indices\":[139,140],\"sizes\":{\"small\":{\"w\":340,\"h\":173,\"resize\":\"fit\"},\"large\":{\"w\":591,\"h\":301,\"resize\":\"fit\"},\"thumb\":{\"w\":150,\"h\":150,\"resize\":\"crop\"},\"medium\":{\"w\":591,\"h\":301,\"resize\":\"fit\"}},\"id_str\":\"568840945341181952\",\"expanded_url\":\"http:\\/\\/twitter.com\\/thebaselinemag\\/status\\/568840945791168512\\/photo\\/1\",\"source_status_id_str\":\"568840945791168512\",\"media_url_https\":\"https:\\/\\/pbs.twimg.com\\/media\\/B-Tt03DCMAALGpK.jpg\",\"id\":568840945341181952,\"source_user_id_str\":\"1363006170\"}],\"user_mentions\":[{\"indices\":[3,18],\"screen_name\":\"thebaselinemag\",\"id_str\":\"1363006170\",\"name\":\" the base line \",\"id\":1363006170}],\"symbols\":[]},\"contributors\":null,\"user\":{\"utc_offset\":null,\"friends_count\":154,\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/567131389951504385\\/i_YsaItu_normal.jpeg\",\"listed_count\":1,\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"default_profile_image\":false,\"favourites_count\":1704,\"created_at\":\"Wed Dec 26 02:28:02 +0000 2012\",\"description\":\"love I try to follow\",\"is_translator\":false,\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"protected\":false,\"screen_name\":\"_kyol\",\"id_str\":\"1036105310\",\"profile_link_color\":\"0084B4\",\"is_translation_enabled\":false,\"geo_enabled\":true,\"id\":1036105310,\"profile_background_color\":\"C0DEED\",\"lang\":\"en\",\"profile_sidebar_border_color\":\"C0DEED\",\"profile_location\":null,\"profile_text_color\":\"333333\",\"verified\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/567131389951504385\\/i_YsaItu_normal.jpeg\",\"time_zone\":null,\"contributors_enabled\":false,\"url\":null,\"profile_background_tile\":false,\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/1036105310\\/1405182978\",\"entities\":{\"description\":{\"urls\":[]}},\"follow_request_sent\":false,\"statuses_count\":4091,\"default_profile\":true,\"followers_count\":151,\"following\":false,\"profile_use_background_image\":true,\"name\":\"Kyle Smith\",\"location\":\"\",\"profile_sidebar_fill_color\":\"DDEEF6\",\"notifications\":false}}\n");

            JSONObject user = (JSONObject) jsonObj.get("user");

            System.out.println(user.get("screen_name"));
            System.out.println(user.get("followers_count"));
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
