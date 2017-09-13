using Nest;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TweetSource.EventSource;
using TweetSource.OAuth;

namespace VW_Twitter_scrape_test
{
    class Program
    {
        private const int reconnectBaseTimeMs = 10000;
        private const int reconnectMaxTimeMs = 240000;
        private static int waitReconectTime = 0;
        public const string twitterDateTemplate = "ddd MMM dd HH:mm:ss +ffff yyyy";
        public static string KEYWORD = "travel";
        public static string INDEX_NAME = $"vw-twitter";
        public static string TYPE = "tweet";
        static ElasticClient elasticClient;

        static async Task MainAsync(string[] args)
        {

            try
            {
                Console.WriteLine("===== Creating Elastic Connection =====");
                var node = new Uri("http://13.79.245.131:9200");
                var settings = new ConnectionSettings(node);
                elasticClient = new ElasticClient(settings);

                Console.WriteLine("===== Creating Elastic Index =====");

                var result = await elasticClient.IndexExistsAsync(new IndexExistsRequest(INDEX_NAME));
                if (!result.Exists)
                {
                    await elasticClient.CreateIndexAsync(INDEX_NAME);
                }

                Console.WriteLine("===== Application Started =====");

                // Step 1: Create TweetEventSource and wire some event handlers.
                var source = TweetEventSource.CreateFilterStream();
                source.EventReceived += new EventHandler<TweetEventArgs>(Source_EventReceived);
                source.SourceUp += new EventHandler<TweetEventArgs>(Source_SourceUp);
                source.SourceDown += new EventHandler<TweetEventArgs>(Source_SourceDown);

                // Step 2: Load the configuration into event source
                Utils.Utils.LoadTwitterKeysFromConfig(source);

                // Step 3: Main loop, e.g. retries 5 times at most
                int retryCount = 0;
                while (retryCount++ < 5)
                {
                    // Step 4: Starts the event source. This starts another thread that pulls data from Twitter to our queue.
                    source.Start(new StreamingAPIParameters()
                    {
                        Track = new string[] { KEYWORD }
                    });

                    // Step 5: While our event source is Active, dispatches events
                    while (source.Active)
                    {
                        source.Dispatch(0); // This fires EventReceived callback on this thread
                        Thread.Sleep(5);
                    }

                    // Step 6: Source is inactive. Ensure stop and cleanup things
                    source.Stop();

                    // Step 7: Wait for some time before attempt reconnect
                    Console.WriteLine("=== Disconnected, wait for {0} ms before reconnect ===", waitReconectTime);
                    Thread.Sleep(waitReconectTime);
                }

                Console.WriteLine("===== Application Ended =====");
            }
            catch (ConfigurationErrorsException cex)
            {
                Console.Error.WriteLine(@"Error reading config: If you're running this for the first time, " +
                    "please make sure you have your version of Twitter.config at application's " +
                    "working directory - " + cex.Message);

                Trace.TraceError("Read config failed: " + cex.ToString());
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine("Unknown error: " + ex.Message);
                Trace.TraceError("Unknown error: " + ex.ToString());
            }
        }

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();


        }
        static void Source_SourceDown(object sender, TweetEventArgs e)
        {
            // At this point, the connection thread ends
            Console.WriteLine("Source is down: " + e.InfoText);
            Trace.TraceInformation("Source is down: " + e.InfoText);

            // Calculate new wait time exponetially
            waitReconectTime = waitReconectTime > 0 ?
                waitReconectTime * 2 : reconnectBaseTimeMs;
            waitReconectTime = waitReconectTime > reconnectMaxTimeMs ?
                reconnectMaxTimeMs : waitReconectTime;
        }

        static void Source_SourceUp(object sender, TweetEventArgs e)
        {
            // Connection established succesfully
            Console.WriteLine("Source is now ready: " + e.InfoText);
            Trace.TraceInformation("Source is now ready: " + e.InfoText);

            // Reset wait time
            waitReconectTime = 0;
        }

        static async void Source_EventReceived(object sender, TweetEventArgs e)
        {
           
            try
            {
                // JSON data from Twitter is in e.JsonText.
                // We parse data using Json.NET by James Newton-King 
                // http://james.newtonking.com/pages/json-net.aspx.
                //
                if (!string.IsNullOrEmpty(e.JsonText))
                {
                    //elasticClient.IndexExists(i => i.Index("myindex"))

                    //var tweet = JObject.Parse(e.JsonText);

                    await elasticClient.LowLevel.IndexAsync<string>(INDEX_NAME, TYPE, e.JsonText);

                    //string screenName = tweet["user"]["screen_name"].ToString();
                    //string text = tweet["text"].ToString();
                    //string createdAt = tweet["created_at"].ToString();

                    //DateTime dateTime = DateTime.ParseExact(createdAt, twitterDateTemplate, new System.Globalization.CultureInfo("en-US"));
                    
                    //var outDateTime = TimeZoneInfo.ConvertTime(dateTime, TimeZoneInfo.Utc, TimeZoneInfo.Local);

                    //text = text.Trim().Replace("\n", "").Replace("\t", "");

                    //Console.WriteLine($"{outDateTime.ToShortDateString()}-{outDateTime.ToShortTimeString()}=>{text}", text);
                    //Console.WriteLine();
                }
            }
            catch (JsonReaderException jex)
            {
                Console.Error.WriteLine("Error JSON read failed: " + jex.Message);
                Trace.TraceError("JSON read failed for text: " + e.JsonText);
                Trace.TraceError("JSON read failed exception: " + jex.ToString());
            }
            catch(Exception ex)
            {
                Console.Error.WriteLine("Error: " + ex.Message);
                Trace.TraceError("Error: " + ex.Message);
            }
        }
    }
}
