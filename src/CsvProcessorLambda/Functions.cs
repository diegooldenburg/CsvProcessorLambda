using System.Globalization;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(
    typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer)
)]

namespace CsvProcessorLambda;

public class Functions
{
    private readonly IAmazonS3 s3Client;

    /// <summary>
    /// Default constructor that Lambda will invoke.
    /// </summary>
    public Functions()
    {
        s3Client = new AmazonS3Client();
    }

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
                foreach (var row in records)
                {
                    var properties = ((IDictionary<string, object>)row).Select(
                        p => $"{p.Key}: {p.Value}"
                    );
                    string logMessage = string.Join(", ", properties);
                    LambdaLogger.Log(logMessage);
                }
            }
        }
        catch (Exception ex)
        {
            LambdaLogger.Log($"Error: {ex.Message}");
        }
    }
}
