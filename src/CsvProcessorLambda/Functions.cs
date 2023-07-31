using System.Globalization;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

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
            context.Logger.LogLine("No S3 record found in the event.");
            LambdaLogger.Log("No S3 record found in the event.");
            return;
        }

        var bucketName = record.Bucket.Name;
        var s3FileName = record.Object.Key;

        context.Logger.LogLine(bucketName);
        context.Logger.LogLine(s3FileName);
        LambdaLogger.Log(bucketName);
        LambdaLogger.Log(s3FileName);

        var getObjectRequest = new GetObjectRequest
        {
            BucketName = bucketName,
            Key = s3FileName
        };

        try
        {
            using (var response = await s3Client.GetObjectAsync(getObjectRequest))
            using (var reader = new StreamReader(response.ResponseStream))
            using (var csv = new CsvReader(reader, new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)))
            {
                var records = csv.GetRecords<dynamic>().ToList();
                foreach (var row in records)
                {
                    context.Logger.LogLine(row.ToString());
                    LambdaLogger.Log(row.ToString());
                }
            }
        }
        catch (Exception ex)
        {
            context.Logger.LogLine($"Error: {ex.Message}");
            LambdaLogger.Log($"Error: {ex.Message}");
        }
    }


    /// <summary>
    /// A Lambda function to respond to HTTP Get methods from API Gateway
    /// </summary>
    /// <remarks>
    /// This uses the <see href="https://github.com/aws/aws-lambda-dotnet/blob/master/Libraries/src/Amazon.Lambda.Annotations/README.md">Lambda Annotations</see> 
    /// programming model to bridge the gap between the Lambda programming model and a more idiomatic .NET model.
    /// 
    /// This automatically handles reading parameters from an APIGatewayProxyRequest
    /// as well as syncing the function definitions to serverless.template each time you build.
    /// 
    /// If you do not wish to use this model and need to manipulate the API Gateway 
    /// objects directly, see the accompanying Readme.md for instructions.
    /// </remarks>
    /// <param name="context">Information about the invocation, function, and execution environment</param>
    /// <returns>The response as an implicit <see cref="APIGatewayProxyResponse"/></returns>
    //     [LambdaFunction(Policies = "AWSLambdaBasicExecutionRole", MemorySize = 256, Timeout = 30)]
    //     [RestApi(LambdaHttpMethod.Get, "/")]
    //     public IHttpResult Get(ILambdaContext context)
    //     {
    //         context.Logger.LogInformation("Handling the 'Get' Request");

    //         return HttpResults.Ok("Hello AWS Serverless");
    //     }
}
