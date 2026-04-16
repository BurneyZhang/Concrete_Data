in powerautomate

1. scheduled run - ever x days
2. save a list of project ids in Morta
3. api call to morta to retrieve the list of projects 


4. api call
```http
POST https://dhk-apims.azure-api.net/alliance-api/v1/Docket/List HTTP/1.1

Content-Type: application/json
Cache-Control: no-cache
Ocp-Apim-Subscription-Key: <redacted>

{
    "projectId": "9350000141",
    "orderDateFrom": "2026-04-02",
    "orderDateTo": "2026-04-02",
    "customerOrderID": ""
}
```
5. save the json to blob storage

6. save log to either of the following options 
    - excel on sharepoint 
        - ❌ only works with powerautomate, or microft linked service 
        - 👎 slow
        - 👎 log stored in a different place -> hard to find , hard to maintain 
        - 👎 limited number of rows
        - 👍 separate place to store 
        - 
    - blob storage -> task -> snowflake 
        - 👎 costly
        - 👎 same pipline as data => no bakcup pipeline, if the main pipeline dont work, the log wont work too.
        - 👍 robust
        - 👍 efficent 
        - 👍 evrything is in the same place 

    - teams message to notify successful run 