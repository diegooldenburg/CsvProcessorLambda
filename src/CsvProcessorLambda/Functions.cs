using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;
using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using YamlDotNet.Serialization;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(
    typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer)
)]

namespace CsvProcessorLambda;

public class Functions
{
    private static readonly IAmazonS3 s3Client = new AmazonS3Client();
    private static readonly AmazonDynamoDBClient dynamoDBClient = new AmazonDynamoDBClient();

    /// <summary>
    /// Default constructor that Lambda will invoke.
    /// </summary>
    public Functions() { }

    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var record = evnt.Records?[0].S3;
        if (record == null)
        {
            LambdaLogger.Log("No S3 record found in the event.");
            return;
        }

        var bucketName = record.Bucket.Name;
        var s3FileName = record.Object.Key;

        var request = new GetItemRequest
        {
            TableName = "YourTableName",
            Key = new Dictionary<string, AttributeValue>() { { "FileName", new AttributeValue { S = s3FileName } } }
        };
        var response = await dynamoDBClient.GetItemAsync(request);
        var options = Document.FromAttributeMap(response.Item);

        var sortByOption = options["SortBy"].AsListOfDocument().Select(
            doc => doc.ToDictionary(kvp => kvp.Key, kvp.Value.AsString())
        ).ToList();

        LambdaLogger.Log(bucketName);
        LambdaLogger.Log(s3FileName);

        var getObjectRequest = new GetObjectRequest { BucketName = bucketName, Key = s3FileName };

        try
        {
            using (var response = await s3Client.GetObjectAsync(getObjectRequest))
            using (var reader = new StreamReader(response.ResponseStream))
            using (
                var csv = new CsvReader(
                    reader,
                    new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
                )
            )
            {
                var records = csv.GetRecords<dynamic>().ToList();

                if (options["DropNull"].AsInt() == 1)
                {
                    records = records.Where(r => !r.Values.Contains(null)).ToList();
                }

                foreach (var sortOption in sortByOption)
                {
                    var propertyName = sortOption["Type"];
                    var order = sortOption["Order"];
                    records = order == "ascending"
                        ? records.OrderBy(r => r[propertyName]).ToList()
                        : records.OrderByDescending(r => r[propertyName]).ToList();
                }

                switch (options["OutputType"].AsString())
                {
                    case "json":
                        output = JsonConvert.SerializeObject(records);
                        break;
                    case "xml":
                        output = new XElement("Root",
                            records.Select(i => new XElement("Record",
                                i.Select(kv => new XElement(kv.Key, kv.Value))))).ToString();
                        break;
                    case "yaml":
                        var serializer = new SerializerBuilder().Build();
                        output = serializer.Serialize(records);
                        break;
                    case "sql":
                        output = "INSERT INTO YourTable (" + string.Join(", ", records.First().Keys) + ") VALUES " +
                            string.Join(", ", records.Select(r => "(" + string.Join(", ", r.Values.Select(v => $"'{v}'")) + ")"));
                        break;
                    default:
                        throw new Exception("Invalid OutputType option.");
                }

                // var putObjectRequest = new PutObjectRequest
                // {
                //     BucketName = bucketName,
                //     Key = s3FileName + "." + options["OutputType"].AsString(),
                //     ContentBody = output
                // };
                // await s3Client.PutObjectAsync(putObjectRequest);

                var logMessages = new ConcurrentDictionary<int, string>();

                Parallel.ForEach(records.Select((value, index) => new { index, value }), row =>
                {
                    var properties = ((IDictionary<string, object>)row.value).Select(
                        p => $"{p.Key}: {p.Value}"
                    );
                    string logMessage = string.Join(", ", properties);
                    logMessages[row.index] = logMessage;
                });

                foreach (var index in logMessages.Keys.OrderBy(i => i))
                {
                    LambdaLogger.Log(logMessages[index]);
                }
            }
        }
        catch (AmazonS3Exception ex)
        {
            LambdaLogger.Log($"S3 Error: {ex.Message}");
        }
        catch (Exception ex)
        {
            LambdaLogger.Log($"General Error: {ex.Message}");
        }
    }
}
