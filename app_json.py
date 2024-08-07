import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb";



const client =  new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

export const handler = async () => {

    var params = {
        TableName: "[TU_TABLA]",
        FilterExpression: "Activo = :valor",
        ReturnConsumedCapacity: "TOTAL",
        ExpressionAttributeValues: {
         ':valor': [CONDICIÓN] 
        },

        
      };
    const command = new ScanCommand(params);
    const response = await docClient.send(command);

 return response;
};
                  
       