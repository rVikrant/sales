
// import aws and make instance for dynamodb
// Load the AWS SDK for Node.js
var AWS = require('aws-sdk');
// Set the region
AWS.config.update({region: 'eu-west-1'});

// Create DynamoDB service object
var dynamodb = new AWS.DynamoDB({apiVersion: "2012-08-10", endpoint: "https://dynamodb.eu-west-1.amazonaws.com"});
// var dbClient = new AWS.DynamoDB.DocumentClient({service: dynamodb});


exports.handler = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false; // callback not to wait for event loop to be empty to return back
    
    console.log('processing event: %j', event.query);
    
    let qryData = event.query,
    date = new Date(),
    dateBeforeFilter = date.getTime() - 86400000,    // date for same time past before the day filter i.e. if 1 day then before 1 day so 2 day before 
    expressionAttrValues = {                    // day filter expression attribute for scan db
        ":date": {
            N: date.getTime().toString()
        }
    },
    filterExpression = "dt >= :date",            // day filter expression 
    expressionAttrValuesBeforeFilterDay = {     // same time past fromthe day filter expressoion attr
        ":date": {
            N: date.getTime().toString()
        },
        ":beforeDateFilter": {
            N: dateBeforeFilter.toString()
        }
    },
    filterExpressionBeforeFilterDay = "dt >= :beforeDateFilter and dt <= :date";   // filter expression for same time past before the filter
    
    console.log("date-----", dateBeforeFilter, date.getTime());
    
    if(qryData) {
        if(qryData.daysFilter) {           // scan db expression on day filter
            let d = new Date(), 
            milisecs = 86400000 * parseInt(qryData.daysFilter),
            date = d.getTime() - milisecs,
            dateBefore = d.getTime() - (milisecs * 2);
            expressionAttrValues[":date"] = {
                N: date.toString()
            };
            expressionAttrValuesBeforeFilterDay = {
                ":date": {
                    N: date.toString()
                },
                ":beforeDateFilter": {
                    N: dateBefore.toString()
                }
            }
        }
        if(qryData.salesRep) {                           // scan db sxpressionon sales rep
            expressionAttrValues[":vertical"] = {
                S: qryData.salesRep
            };
            expressionAttrValuesBeforeFilterDay[":vertical"] = {
                S: qryData.salesRep
            };
            
            filterExpression += " AND vertical = :vertical";
            filterExpressionBeforeFilterDay += " AND vertical = :vertical";
            
        }
    }
    
    // get data 
    Promise.all([
        getDataFromDB(dynamodb, filterExpression, expressionAttrValues), 
        getDataFromDB(dynamodb, filterExpressionBeforeFilterDay, expressionAttrValuesBeforeFilterDay),
        getDataForTopSalesRep(dynamodb, date, filterExpression, expressionAttrValues)
    ])
    .then(dbRes => {
        let data = dbRes[0];
          Promise.all([
              getEngagementStatusData(data.Items),
              getCallAndWonData(dbRes[1].Items, dbRes[0].Items)
              ])
              .then(res => {
                  console.log("engagementStatus===", res[0], res[1], JSON.stringify(dbRes[2]));
                  callback(null, {
                      data: data.Items,
                      engagementStatus: res[0],
                      callsWinsAndRevenue: res[1],
                      topBottomSalesRep: dbRes[2]
                      
                    })
              })
              .catch(e => {
                  callback(null, {error: e, statusCode: 400})
              });
    });
    
}

// dynamod scan query 
let getDataFromDB = (dynamodb,filterExpression, expressionAttrValues) => {
    return new Promise((resolve, reject) => {
        dynamodb.scan({
        //   Segment: 2,
          TableName: 'sales',
        //   TotalSegments : 5,              // parallel scaning in the table
          ExpressionAttributeValues: expressionAttrValues,
          FilterExpression: filterExpression
        }, (err, data) => {
            console.log("scan items----", err, data);
            err ? reject(err) : resolve(data);
        })
    })
}

// fetch engagement status data for sales rep
let getEngagementStatusData = async (items) => {
    let leadIn = 0, negotiations = 0, proposals = 0, needsToDefine = 0, contactMade = 0,
    won = 0, calls = 0;
    for(let item of items) {
        console.log(item.engagementStatus.S, "statussssssss");
        if(item.meetingLocation.S === "Call") calls++;
        switch (item.engagementStatus.S) {
            case 'Won': {
                won++;
                break;
            }
             case 'Lead In': {
                leadIn++;
                break;
            }
            case 'Contact Made': {
                contactMade++;
                break;
            }
            case 'Proposal Made': {
                proposals++;
                break;
            }
            case 'Needs Defined': {
                needsToDefine++;
                break;
            }
            case 'Negotiations Started': {
                negotiations++;
                break;
            }
        }
    }
    return {
        won: won,
        calls: calls,
        leadIn: leadIn,
        contactMade: contactMade,
        proposalMade: proposals,
        needToDefine: needsToDefine,
        negotiationStarted: negotiations
    };
}

// calls and won data for comparison from last days to current filter
let getCallAndWonData = async (items, currentDayFilterItems) => {
    let calls = 0, won = 0, revenue = 0, currentTimeWon = 0, currentTimeRevenue = 0, currentTimeCalls = 0;
    for(let item of items) {
        if(item.meetingLocation.S === "Call") calls++;
        if(item.engagementStatus.S === 'Won') won++;
        if(item.revenue.N) revenue += parseInt(item.revenue.N);
    }
    
    for(let item of currentDayFilterItems) {
        if(item.meetingLocation.S === "Call") currentTimeCalls++;
        if(item.engagementStatus.S === 'Won') currentTimeWon++;
        if(item.revenue.N) currentTimeRevenue += parseInt(item.revenue.N);
    }
    
    return [
        {
            image:'assets/img/ico-wins.png',
            label:'wins',
            value:currentTimeWon,
            total:won
        },
        {
            image:'assets/img/ico-calls.png',
            label:'calls',
            value:currentTimeCalls,
            total:calls
        },
        {
            image:'assets/img/ico-revenue.png',
            label:'incremented revenue',
            value:currentTimeRevenue,
            total:revenue
        }
    ]
}

// top and last sales reps as per the demo calls  
// fetch all sales Reps -> get their sales data -> compare data and resolve output
let getDataForTopSalesRep = (dynamodb, date, filterExpression, expressionAttrValues) => {
    return new Promise((resolve, reject) => {
        dynamodb.scan({
            TableName: 'salespersons',
            ProjectionExpression: "id",
            // ExpressionAttributeValues: {
            //     ":date": {
            //         N: date.getTime().toString()
            //     }
            // },
            // FilterExpression: "dt <= :date"
        }, (err, salesReps) => {
            if(err) return reject(err);
            
            console.log("sales resps---", salesReps)
            let top = [{demoCalls: 0, newLogos: 0, name: '', newMrr:5230}, {demoCalls: 0, newLogos: 0, name: '', newMrr:5230}],
            bottom = [{demoCalls: 0, newLogos: 0, name: '', newMrr:5230}, {demoCalls: 0, newLogos: 0, name: '', newMrr:5230}], 
            items = salesReps.Items, itemsLen = items.length;
            for(let i = 0; i < itemsLen; i++) {
                let id = items[i].id.N.toString(),
                filterExpressForDemoCalls = filterExpression + " AND meetingLocation = :meetingLocation AND salesRepId = :id" ,
                filterExpressForNewLogos = filterExpression + " AND clientNew = :clientNew AND salesRepId = :id", 
                expressionAttrValuesForNewLogos = { ...expressionAttrValues },
                expressionAttrValuesForDemoCalls  = { ...expressionAttrValues };
                
                expressionAttrValuesForDemoCalls[":meetingLocation"] =  { S: "Call"};
                expressionAttrValuesForDemoCalls[":id"] =  {N: id};
                expressionAttrValuesForNewLogos[":clientNew"] = { BOOL: true};
                expressionAttrValuesForNewLogos[":id"] =  {N: id};
                
                Promise.all([
                    getDataFromDB(dynamodb, filterExpressForDemoCalls, expressionAttrValuesForDemoCalls),
                    getDataFromDB(dynamodb, filterExpressForNewLogos, expressionAttrValuesForNewLogos),
                ])
                .then(res => {
                    let callsItems = res[0].Items, calls = callsItems.length, 
                    clientsItems = res[1].Items, clientsNew = clientsItems.length;
                    
                    console.log("res----", callsItems, clientsItems);
                    console.log("res---111-", callsItems[0]);
                    
                    // incase of top value change -> assign top value pos to 2nd top pos and update top pos 
                    if(top[0].demoCalls <= calls) {
                        top[1].demoCalls = top[0].demoCalls;
                        top[1].newLogos = top[0].newLogos ;
                        top[1].name = top[0].name;
                        top[1].salesRepId = top[0].salesRepId;
                        top[0].demoCalls = calls;
                        top[0].newLogos = clientsNew ;
                        if(calls) top[0].name = callsItems[0].salesRep.S;
                        else if(clientsNew) top[0].name = clientsItems[0].salesRep.S;
                        if(calls) top[0].salesRepId = callsItems[0].salesRepId.N;
                        else if(clientsNew) top[0].salesRepId = clientsItems[0].salesRepId.N;
                    }
                    if(top[1].demoCalls < calls && top[0].demoCalls > calls) {
                        top[1].demoCalls = calls;
                        top[1].newLogos = clientsNew ;
                        top[1].name = callsItems[0].salesRep.S;
                        top[1].salesRepId = callsItems[0].salesRepId.N
                    }
                    
                    // incase of top value change -> assign top value pos to 2nd top pos and update top pos
                    if(bottom[0].demoCalls >= calls) {
                        bottom[1].demoCalls = bottom[0].demoCalls;
                        bottom[1].newLogos = bottom[0].newLogos ;
                        bottom[1].name = bottom[0].name;
                        bottom[1].salesRepId = bottom[0].salesRepId;
                        bottom[0].demoCalls = calls;
                        bottom[0].newLogos = clientsNew ;
                        if(calls) bottom[0].name = callsItems[0].salesRep.S;
                        else if(clientsNew) bottom[0].name = clientsItems[0].salesRep.S;
                        if(calls) bottom[0].salesRepId = callsItems[0].salesRepId.N;
                        else if(clientsNew) bottom[0].salesRepId = clientsItems[0].salesRepId.N;
                    }
                    if(bottom[1].demoCalls > calls && bottom[0].demoCalls < calls) {
                        bottom[1].demoCalls = calls;
                        bottom[1].newLogos = clientsNew ;
                        bottom[1].name = callsItems[0].salesRep.S;
                        bottom[1].salesRepId = callsItems[0].salesRepId.N
                    }
                    
                    if(i === itemsLen-1) resolve({top: top, bottom: bottom})
                })
                .catch(e => {
                    console.log("err in top bottom data fetch----", e)
                    reject(e)
                })
            }
        })
    })
}
