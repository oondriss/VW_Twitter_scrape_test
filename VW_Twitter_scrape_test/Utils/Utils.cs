using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TweetSource.EventSource;

namespace VW_Twitter_scrape_test.Utils
{
    public static class Utils
    {
        public static void LoadTwitterKeysFromConfig(TweetEventSource source)
        {
            var settings = System.Configuration.ConfigurationManager.AppSettings;
            var config = source.AuthConfig;

            config.ConsumerKey = settings["ConsumerKey"];
            config.ConsumerSecret = settings["ConsumerSecret"];
            config.Token = settings["Token"];
            config.TokenSecret = settings["TokenSecret"];

            // These are default values:
            // config.OAuthVersion = "1.0";
            // config.SignatureMethod = "HMAC-SHA1";
#if DEBUG
            Console.WriteLine(config.ToString());
#endif
        }
    }
}
