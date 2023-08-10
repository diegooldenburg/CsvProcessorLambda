using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using CsvHelper;
using Utf8Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
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
            TableName = "CsvProcessingOptions",
            Key = new Dictionary<string, AttributeValue>()
            {
                {
                    "FileName",
                    new AttributeValue { S = s3FileName }
                }
            }
        };

        GetItemResponse dynamoResponse;
        try
        {
            dynamoResponse = await dynamoDBClient.GetItemAsync(request);
        }
        catch (Exception ex)
        {
            LambdaLogger.Log($"Error retrieving item from DynamoDB: {ex.Message}");
            return;
        }

        if (!dynamoResponse.IsItemSet)
        {
            LambdaLogger.Log($"No item found in DynamoDB for file name: {s3FileName}");
            return;
        }

        var options = dynamoResponse.Item;

        List<Dictionary<string, string>> sortByOption = null;
        if (options.ContainsKey("SortBy") && options["SortBy"].IsLSet)
        {
            sortByOption = options["SortBy"].L
                .Select(av => av.M.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.S))
                .ToList();
        }

        LambdaLogger.Log(bucketName);
        LambdaLogger.Log(s3FileName);

        var getObjectRequest = new GetObjectRequest { BucketName = bucketName, Key = s3FileName };

        try
        {
            string output;

            using (var s3Response = await s3Client.GetObjectAsync(getObjectRequest))
            using (var reader = new StreamReader(s3Response.ResponseStream))
            using (
                var csv = new CsvReader(
                    reader,
                    new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
                )
            )
            {
                var records = csv.GetRecords<dynamic>().ToList();

                AttributeValue dropNull;
                if (options.TryGetValue("DropNull", out dropNull) && dropNull.S != null)
                {
                    if (bool.Parse(dropNull.S))
                    {
                        records = records
                            .Where(r => !((IDictionary<string, object>)r).Values.Contains(null))
                            .ToList();
                    }
                }

                switch (options["OutputType"].S)
                {
                    case "json":
                        output = Utf8Json.JsonSerializer.ToJsonString(records);
                        break;
                    case "xml":
                        output = new XElement(
                            "Root",
                            records.Select(
                                i =>
                                    new XElement(
                                        "Record",
                                        ((IDictionary<string, object>)i).Select(
                                            kv => new XElement(kv.Key, kv.Value)
                                        )
                                    )
                            )
                        ).ToString();
                        break;
                    case "yaml":
                        var serializer = new SerializerBuilder().Build();
                        output = serializer.Serialize(records);
                        break;
                    case "sql":
                        output =
                            "INSERT INTO YourTable ("
                            + string.Join(", ", records.First().Keys)
                            + ") VALUES "
                            + string.Join(
                                ", ",
                                records.Select(
                                    r =>
                                        "("
                                        + string.Join(
                                            ", ",
                                            ((IDictionary<string, object>)r).Values.Select(
                                                v => $"'{v}'"
                                            )
                                        )
                                        + ")"
                                )
                            );
                        break;
                    default:
                        throw new Exception("Invalid OutputType option.");
                }

                var putObjectRequest = new PutObjectRequest
                {
                    BucketName = "processed-csv",
                    Key =
                        Path.GetFileNameWithoutExtension(s3FileName)
                        + "."
                        + options["OutputType"].S,
                    ContentBody = output
                };
                await s3Client.PutObjectAsync(putObjectRequest);
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
