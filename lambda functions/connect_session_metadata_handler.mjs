// Connect Lambda — stores contact attributes in DynamoDB for the SMA Lambda to pick up
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";

const ddb = new DynamoDBClient({});
const TABLE_NAME = process.env.METADATA_TABLE;

export const handler = async (event) => {
  console.log("Connect event:", JSON.stringify(event));

  const contactId = event.Details?.ContactData?.ContactId || "unknown";
  const callerNumber = event.Details?.ContactData?.CustomerEndpoint?.Address || "unknown";
  const attributes = event.Details?.ContactData?.Attributes || {};

  // Store all user-defined contact attributes
  const item = {
    CallerNumber: { S: callerNumber },
    Timestamp: { N: String(Date.now()) },
    ContactId: { S: contactId },
    Attributes: { S: JSON.stringify(attributes) },
    TTL: { N: String(Math.floor(Date.now() / 1000) + 300) }, // 5 min TTL
  };

  await ddb.send(new PutItemCommand({ TableName: TABLE_NAME, Item: item }));

  console.log("Stored metadata for", callerNumber, "contactId:", contactId);

  // Return attributes back to Connect (can be used in the flow)
  return { statusCode: 200, callerNumber };
};