#include "server.h"
#include <sys/time.h>
#include <math.h>
#include <string.h>

zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank);
int hashTypeGetFromZiplist(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll);
sds hashTypeGetFromHashTable(robj *o, sds field);

long long getCurrentTimeStampLongLong();
void getCurrentTimeStampString(char *timeStampStr);
void singleSetAddWithKeyObj(client *c,robj *theKey, robj *setobj ,const sds member);
void singleZAddWithKeyObj(client *c,robj *theKey,robj *zobj, const sds member, long double score);
void GetDataFromHash(robj *o, sds field, sds *returnResult);
void GetDataFromHash88Replace(robj *o, sds field, sds *returnResult);
void DeleteSingleMember(client *c, robj *zobj, robj *theKey, sds memberName);
/* function use to add record */
void AddOrderHashRecord(client *c, char *myOrderId, char *orderBuySell, long double CurrentAmount,sds *keyFieldsds, robj *keyFieldKey,sds *getsetValueField)
{
    size_t myOrderlen = strlen(myOrderId);
    *keyFieldsds = sds88replace(*keyFieldsds,myOrderId, myOrderlen);
    robj *orderRecordObj = lookupKeyWrite(c->db, keyFieldKey);
    char tempCurrentAmountStr[25];
    sprintf(tempCurrentAmountStr,c->argv[4]->ptr,CurrentAmount);
    
    /*  currentAmountStr = sdscatprintf(currentAmountStr, amountDecimalFormat, CurrentAmount);*/

    int created = 0;
    if (orderRecordObj == NULL)
    {
        orderRecordObj = createHashObject();
        dbAdd(c->db, keyFieldKey, orderRecordObj);
    }
    else
    {
        if (orderRecordObj->type != OBJ_HASH)
        {
            int deleted = dbSyncDelete(c->db, keyFieldKey);
            if (deleted)
            {
                signalModifiedKey(c->db, keyFieldKey);
                server.dirty++;
            }
            orderRecordObj = createHashObject();
            dbAdd(c->db, keyFieldKey, orderRecordObj);
        }
    }
    
    *keyFieldsds = sds88replace(*keyFieldsds,"User_Id",7);

    created += !hashTypeSet(orderRecordObj, *keyFieldsds, c->argv[2]->ptr, HASH_SET_COPY);
   
   

    *keyFieldsds = sds88replace(*keyFieldsds,"Order_Id",8);
    *getsetValueField = sds88replace(*getsetValueField,myOrderId,strlen(myOrderId));
    created += !hashTypeSet(orderRecordObj, *keyFieldsds, *getsetValueField, HASH_SET_COPY);


    *keyFieldsds = sds88replace(*keyFieldsds,"BuySell",7);
    *getsetValueField = sds88replace(*getsetValueField,orderBuySell,strlen(orderBuySell));
    created += !hashTypeSet(orderRecordObj, *keyFieldsds, *getsetValueField, HASH_SET_COPY); 

    *keyFieldsds = sds88replace(*keyFieldsds,"Market",6);
    created += !hashTypeSet(orderRecordObj, *keyFieldsds, c->argv[1]->ptr, HASH_SET_COPY);

    *keyFieldsds = sds88replace(*keyFieldsds,"Price",5);
    created += !hashTypeSet(orderRecordObj, *keyFieldsds, c->argv[5]->ptr, HASH_SET_COPY);
   
    *keyFieldsds = sds88replace(*keyFieldsds,"Amount",6);
    created += !hashTypeSet(orderRecordObj, *keyFieldsds, c->argv[6]->ptr, HASH_SET_COPY);

    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);
    *getsetValueField = sds88replace(*getsetValueField,tempCurrentAmountStr,strlen(tempCurrentAmountStr));
    created += !hashTypeSet(orderRecordObj, *keyFieldsds, *getsetValueField, HASH_SET_COPY);


    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLastLeft",14);
    created += !hashTypeSet(orderRecordObj, *keyFieldsds, c->argv[6]->ptr, HASH_SET_COPY);

   
    *keyFieldsds = sds88replace(*keyFieldsds,myOrderId, myOrderlen);
    signalModifiedKey(c->db, keyFieldKey);
    server.dirty++;

    
    return;
}
int IsPriceInFirstTenBidAskRange(client *c,  long double price,sds *keyFieldsds,robj *keyFieldKey)
{   
    char marketBid[20];
    char marketAsk[20];
    stpcpy(stpcpy(marketBid,c->argv[1]->ptr),"BID");
    stpcpy(stpcpy(marketAsk,c->argv[1]->ptr),"ASK");

    /*zrange  10th record from ask and zremrange from bid*/
    *keyFieldsds = sds88replace(*keyFieldsds,marketBid, strlen(marketBid)); 
    robj *marketBidObj = lookupKeyRead(c->db, keyFieldKey);
   
    long double bidprice = 0, askprice = 9999999;
    long start = 9;
    int llen;
    if (marketBidObj != NULL &&  marketBidObj->type == OBJ_ZSET)
    {      
        llen = zsetLength(marketBidObj);
        /*if less than 10 obj take the last one*/
        if (start >= llen)
        {
            start = llen - 1;
        }
        if (marketBidObj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = marketBidObj->ptr;
            unsigned char *eptr, *sptr;

            eptr = ziplistIndex(zl, -2 - (2 * start));
            sptr = ziplistNext(zl, eptr);
            bidprice = zzlGetScore(sptr);
        }
        else if (marketBidObj->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = marketBidObj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;

            ln = zsl->tail;
            if (start > 0)
            {
                ln = zslGetElementByRank(zsl, llen - start);
            }

            if (ln != NULL)
            {
                bidprice = ln->score;
            }
        }
    }
    *keyFieldsds = sds88replace(*keyFieldsds,marketAsk, strlen(marketAsk));
    robj *marketAskObj = lookupKeyRead(c->db, keyFieldKey);
    if (marketAskObj != NULL && marketAskObj->type == OBJ_ZSET)
    {        
        start = 9;
        llen = zsetLength(marketAskObj);
        if (start >= llen)
        {
            start = llen - 1;
        }
        if (marketAskObj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = marketAskObj->ptr;
            unsigned char *eptr, *sptr;


            eptr = ziplistIndex(zl, 2 * start);
            sptr = ziplistNext(zl, eptr);
            askprice = zzlGetScore(sptr);
        }
        else if (marketAskObj->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = marketAskObj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            ln = zsl->header->level[0].forward;
            if (start > 0)
            {
                ln = zslGetElementByRank(zsl, start + 1);
            }
            if (ln != NULL)
            {
                askprice = ln->score;
            }
        }
    }

    return (price >= bidprice && price <= askprice);
   
}
void QueryBidAsk(client *c, long long number, sds *bidaskString, sds *keyFieldsds,robj *keyFieldKey )
{
  
    char *tempstrtodigitPtr;
    robj *bidzobj;
    robj *askzobj;
    int llen;
    size_t curlen;

    char amountMarketPrice[100];
    char tempAmount[40];
    char tempPrice[40];
    char tempBidAskStr[80];

    char marketBid[20];
    char marketAsk[20];
    
    stpcpy(stpcpy(marketBid,c->argv[1]->ptr),"BID");
    stpcpy(stpcpy(marketAsk,c->argv[1]->ptr),"ASK");
    
    char tempmarketString[40];

    stpcpy(stpcpy(stpcpy(tempmarketString,"{\"Market\":\""),c->argv[1]->ptr),"\",\"Bid\":[");
    
    *bidaskString = sds8kreplace(*bidaskString,tempmarketString,strlen(tempmarketString));
    *keyFieldsds = sds88replace(*keyFieldsds,marketBid, strlen(marketBid));
    

    if ((bidzobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && bidzobj->type == OBJ_ZSET)
    {    
        llen = zsetLength(bidzobj);
        if (number < llen)
            llen = number;

        if (bidzobj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = bidzobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            eptr = ziplistIndex(zl, -2);
            sptr = ziplistNext(zl, eptr);
           
            while (llen--)
            {                
                ziplistGet(eptr, &vstr, &vlen, &vlong);
                if (vstr != NULL)
                {
                    strncpy(amountMarketPrice,(char *)vstr,vlen);
                    amountMarketPrice[vlen]='\0';
                    tempstrtodigitPtr = NULL;
                    strcpy(tempAmount,strtok_r(amountMarketPrice, marketBid,&tempstrtodigitPtr));
			        strcpy(tempPrice,strtok_r(NULL, marketBid,&tempstrtodigitPtr));                    
                    stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                    *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));

                    
                }               
                zzlPrev(zl, &eptr, &sptr);
                if (llen == 0)
                {                    
                    *bidaskString = sdscat8klen(*bidaskString, "}",1);
                }
                else
                {
                    *bidaskString = sdscat8klen(*bidaskString, "},",2);
                }
            }
        }
        else if (bidzobj->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = bidzobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            sds ele;
            ln = zsl->tail;
            while (llen--)
            {
                ele = ln->ele;
                curlen = sdslen(ele);
                strncpy(amountMarketPrice,ele,curlen);
                amountMarketPrice[curlen]='\0';

                tempstrtodigitPtr = NULL;
                strcpy(tempAmount,strtok_r(amountMarketPrice, marketBid,&tempstrtodigitPtr));
			    strcpy(tempPrice,strtok_r(NULL, marketBid,&tempstrtodigitPtr));
                stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));
                
                if (llen == 0)
                {
                    *bidaskString = sdscat8klen(*bidaskString, "}",1);
                }
                else
                {
                    *bidaskString = sdscat8klen(*bidaskString, "},",2);
                }
                ln = ln->backward;
            }
        }
       
    }
    
    *bidaskString = sdscat8klen(*bidaskString, "],\"Ask\":[",9);
    *keyFieldsds = sds88replace(*keyFieldsds,marketAsk, strlen(marketAsk));
    if ((askzobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && askzobj->type == OBJ_ZSET)
    {
        llen = zsetLength(askzobj);
        if (number < llen)
           llen = number;

        if (askzobj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = askzobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            eptr = ziplistIndex(zl, 0);
            sptr = ziplistNext(zl, eptr);

            while (llen--)
            {               
                ziplistGet(eptr, &vstr, &vlen, &vlong);
                if (vstr != NULL)
                {
                    strncpy(amountMarketPrice,(char *)vstr,vlen);
                    amountMarketPrice[vlen]='\0';

                    tempstrtodigitPtr = NULL;
                    strcpy(tempAmount,strtok_r(amountMarketPrice, marketAsk,&tempstrtodigitPtr));
			        strcpy(tempPrice,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));                    
                    
                    stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                    *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));
                   
                    
                }
                
                zzlNext(zl, &eptr, &sptr);
                if (llen == 0)
                {
                    *bidaskString = sdscat8klen(*bidaskString, "}",1);
                }
                else
                {
                    *bidaskString = sdscat8klen(*bidaskString, "},",2);
                }
            }
        }
        else if (askzobj->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = askzobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            sds ele;

            /* Check if starting point is trivial, before doing log(N) lookup. */
            ln = zsl->header->level[0].forward;
            while (llen--)
            {               
                ele = ln->ele;
                curlen = sdslen(ele);
                strncpy(amountMarketPrice,ele,curlen);
                amountMarketPrice[curlen]='\0';

                tempstrtodigitPtr = NULL;
                strcpy(tempAmount,strtok_r(amountMarketPrice, marketAsk,&tempstrtodigitPtr));
			    strcpy(tempPrice,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));
                stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));
                
                        
                ln = ln->level[0].forward;
                if (llen == 0)
                {
                    *bidaskString = sdscat8klen(*bidaskString, "}",1);
                }
                else
                {
                    *bidaskString = sdscat8klen(*bidaskString, "},",2);
                }
            }
        }        
    }
    
    *bidaskString = sdscat8klen(*bidaskString, "]}",2);

}
void QueryBidAskMulti(client *c,  long long number, sds *bidaskString,sds *publishString, sds *keyFieldsds,robj *keyFieldKey ,sds *getsetValueField)
{
   
    char *tempstrtodigitPtr;
    robj *bidzobj;
    robj *askzobj;
    int llen;
    size_t curlen;

    char amountMarketPrice[100];
    char tempAmount[40];
    char tempPrice[40];
    char tempBidAskStr[80];

    char marketBid[20];
    char marketAsk[20];
    
    stpcpy(stpcpy(marketBid,c->argv[1]->ptr),"BID");
    stpcpy(stpcpy(marketAsk,c->argv[1]->ptr),"ASK");
    
    char tempmarketString[40];

    stpcpy(stpcpy(stpcpy(tempmarketString,"{\"Market\":\""),c->argv[1]->ptr),"\",\"Bid\":[");
    
    *bidaskString = sds8kreplace(*bidaskString,tempmarketString,strlen(tempmarketString));
    *publishString = sds8kreplace(*publishString,tempmarketString,strlen(tempmarketString));
    *keyFieldsds = sds88replace(*keyFieldsds,marketBid, strlen(marketBid));
    

    if ((bidzobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && bidzobj->type == OBJ_ZSET)
    {    
        llen = zsetLength(bidzobj);
        if (number < llen)
            llen = number;

        if (bidzobj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = bidzobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            eptr = ziplistIndex(zl, -2);
            sptr = ziplistNext(zl, eptr);
           
            while (llen--)
            {                
                ziplistGet(eptr, &vstr, &vlen, &vlong);
                if (vstr != NULL)
                {
                    strncpy(amountMarketPrice,(char *)vstr,vlen);
                    amountMarketPrice[vlen]='\0';
                    tempstrtodigitPtr = NULL;
                    strcpy(tempAmount,strtok_r(amountMarketPrice, marketBid,&tempstrtodigitPtr));
			        strcpy(tempPrice,strtok_r(NULL, marketBid,&tempstrtodigitPtr));                    
                    stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                    *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));
                    *publishString = sdscat8klen(*publishString, tempBidAskStr,strlen(tempBidAskStr));

                    
                    /*loop for id*/
                    *publishString = sdscat8klen(*publishString, ",\"UserId\":[",11);
                    char marketPrice[50];
                    stpcpy(stpcpy(marketPrice,marketBid), tempPrice);
                    *keyFieldsds = sds88replace(*keyFieldsds,marketPrice, strlen(marketPrice));
                    
                    robj *marketPricezobj;

                    int ttl;
                    if ((marketPricezobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && marketPricezobj->type == OBJ_ZSET)
                    {                            
                        ttl = zsetLength(marketPricezobj);
                        if (marketPricezobj->encoding == OBJ_ENCODING_ZIPLIST)
                        {
                            unsigned char *zl2 = marketPricezobj->ptr;
                            unsigned char *eptr2, *sptr2;
                            unsigned char *vstr2;
                            unsigned int vlen2;
                            long long vlong2;

                            int first = 1;
                            eptr2 = ziplistIndex(zl2, 0);
                            sptr2 = ziplistNext(zl2, eptr2);

                            char tempOrderId[30];
                            while (ttl--)
                            {
                                ziplistGet(eptr2, &vstr2, &vlen2, &vlong2);
                                if (vstr2 == NULL)
                                {   
                                    sprintf(tempOrderId, "%lld", vlong2);
                                }
                                else
                                {
                                    strncpy(tempOrderId,(char *)vstr2,vlen2);
                                    tempOrderId[vlen2] ='\0';
                                }
                                *keyFieldsds = sds88replace(*keyFieldsds,tempOrderId, strlen(tempOrderId));                                    
                                robj *orderobj;
                                orderobj = lookupKeyWrite(c->db, keyFieldKey);
                                if (orderobj != NULL)
                                {                                 
                                    *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);
                                    curlen = sdslen(*publishString);
                                    if (first)
                                    {
                                        publishString[0][curlen] = '\"';
                                        publishString[0][curlen+1] = '\0';
                                        sdssetlen(*publishString,curlen+1);
                                        first = 0;
                                    }
                                    else{
                                        publishString[0][curlen] = ',';
                                        publishString[0][curlen+1] = '\"';
                                        publishString[0][curlen+2] = '\0';
                                        sdssetlen(*publishString,curlen+2);
                                    }                                      
                                    
                                    GetDataFromHash(orderobj, *getsetValueField, publishString);

                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = '(';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);
                                    

                                    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);
                                    GetDataFromHash88Replace(orderobj, *keyFieldsds, getsetValueField);
                                    
                                    long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);
                                    if (amountLeft >= 1000)
                                    {
                                        amountLeft = amountLeft / 1000;
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                        *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                    }
                                    else
                                    {
                                        *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                    }
                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = ')';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);                                                                          
                                }
                                zzlNext(zl, &eptr2, &sptr2);
                            }                                
                        }
                        else if (marketPricezobj->encoding == OBJ_ENCODING_SKIPLIST)
                        {
                            zset *zs = marketPricezobj->ptr;
                            zskiplist *zsl = zs->zsl;
                            zskiplistNode *ln;
                            sds ele;
                            int first = 1;
                            ln = zsl->header->level[0].forward;
                            while (ttl--)
                            {
                                ele = ln->ele;
                                *keyFieldsds = sds88replace(*keyFieldsds,ele, sdslen(ele)); 
                                robj *orderobj;
                                orderobj = lookupKeyWrite(c->db, keyFieldKey);
                                if (orderobj != NULL)
                                {
                                    *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);  
                                    curlen = sdslen(*publishString);                                      
                                    if (first)
                                    {
                                        publishString[0][curlen] = '\"';
                                        publishString[0][curlen+1] = '\0';
                                        sdssetlen(*publishString,curlen+1);
                                        first = 0;
                                    }
                                    else{
                                        publishString[0][curlen] = ',';
                                        publishString[0][curlen+1] = '\"';
                                        publishString[0][curlen+2] = '\0';
                                        sdssetlen(*publishString,curlen+2);
                                    }                                     
                                    GetDataFromHash(orderobj, *getsetValueField, publishString);

                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = '(';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);

                                    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);                                                               
                                    GetDataFromHash88Replace(orderobj, *keyFieldsds,getsetValueField);
                                    long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);                                        
                                    if (amountLeft >= 1000)
                                    {
                                        amountLeft = amountLeft / 1000;
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                        *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                    }
                                    else
                                    {
                                        *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                    }
                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = ')';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);                                      
                                }                                    
                                ln = ln->level[0].forward;
                            }
                        }                           
                    }
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = ']';
                    publishString[0][curlen+1] = '\0';
                    sdssetlen(*publishString,curlen+1); 
                    
                }               
                zzlPrev(zl, &eptr, &sptr);
                if (llen == 0)
                {                    
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = '\0';
                    sdssetlen(*publishString,curlen+1); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = '\0';
                    sdssetlen(*bidaskString,curlen+1); 
                }
                else
                {
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = ',';
                    publishString[0][curlen+2] = '\0';
                    sdssetlen(*publishString,curlen+2); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = ',';
                    bidaskString[0][curlen+2] = '\0';
                    sdssetlen(*bidaskString,curlen+2); 
                }
            }
        }
        else if (bidzobj->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = bidzobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            sds ele;
            /* Check if starting point is trivial, before doing log(N) lookup. */
            ln = zsl->tail;
            while (llen--)
            {
                ele = ln->ele;
                curlen = sdslen(ele);
                strncpy(amountMarketPrice,ele,curlen);
                amountMarketPrice[curlen]='\0';

                tempstrtodigitPtr = NULL;
                strcpy(tempAmount,strtok_r(amountMarketPrice, marketBid,&tempstrtodigitPtr));
			    strcpy(tempPrice,strtok_r(NULL, marketBid,&tempstrtodigitPtr));
                stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));
                *publishString = sdscat8klen(*publishString, tempBidAskStr,strlen(tempBidAskStr));
                
                /*loop for id*/
                *publishString = sdscat8klen(*publishString, ",\"UserId\":[",11);
                char marketPrice [50];
                stpcpy(stpcpy(marketPrice,marketBid), tempPrice);
                *keyFieldsds = sds88replace(*keyFieldsds,marketPrice, strlen(marketPrice));
                    
                robj *marketPricezobj;

                int ttl;
                if ((marketPricezobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && marketPricezobj->type == OBJ_ZSET)
                {
                    ttl = zsetLength(marketPricezobj);
                    if (marketPricezobj->encoding == OBJ_ENCODING_ZIPLIST)
                    {
                        unsigned char *zl = marketPricezobj->ptr;
                        unsigned char *eptr, *sptr;
                        unsigned char *vstr;
                        unsigned int vlen;
                        long long vlong;

                        int first = 1;
                        eptr = ziplistIndex(zl, 0);
                        sptr = ziplistNext(zl, eptr);
                        
                        char tempOrderId[30];
                        while (ttl--)
                        {
                            
                            ziplistGet(eptr, &vstr, &vlen, &vlong);
                            if (vstr == NULL)
                            {
                                    sprintf(tempOrderId, "%lld", vlong);
                            }
                            else
                            {
                                strncpy(tempOrderId,(char *)vstr,vlen);
                                tempOrderId[vlen] ='\0';
                            }
                            *keyFieldsds = sds88replace(*keyFieldsds,tempOrderId, strlen(tempOrderId));  
                            robj *orderobj;
                            orderobj = lookupKeyWrite(c->db, keyFieldKey);
                            if (orderobj != NULL)
                            {
                                
                                *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);
                                curlen = sdslen(*publishString);
                                if (first)
                                {
                                    publishString[0][curlen] = '\"';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);
                                    first = 0;
                                }
                                else{
                                    publishString[0][curlen] = ',';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);
                                    
                                }
                                GetDataFromHash(orderobj, *getsetValueField, publishString);

                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = '(';
                                publishString[0][curlen+1] = '\0';
                                sdssetlen(*publishString,curlen+1);

                                *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);              
                                GetDataFromHash88Replace(orderobj, *keyFieldsds, getsetValueField);
                                long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);
                                if (amountLeft >= 1000)
                                {
                                    amountLeft = amountLeft / 1000;
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                    *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                }
                                else
                                {
                                    *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                }
                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = ')';
                                publishString[0][curlen+1] = '\"';
                                publishString[0][curlen+2] = '\0';
                                sdssetlen(*publishString,curlen+2); 
                            } 
                            zzlNext(zl, &eptr, &sptr);
                        }
                    }
                    else if (marketPricezobj->encoding == OBJ_ENCODING_SKIPLIST)
                    {
                        zset *zs2 = marketPricezobj->ptr;
                        zskiplist *zsl2 = zs2->zsl;
                        zskiplistNode *ln2;
                        sds ele2;
                        int first = 1;
                        ln2 = zsl2->header->level[0].forward;
                        while (ttl--)
                        {

                            ele2 = ln2->ele;
                            *keyFieldsds = sds88replace(*keyFieldsds,ele2, sdslen(ele2)); 
                            robj *orderobj;
                            orderobj = lookupKeyWrite(c->db, keyFieldKey);
                            if (orderobj != NULL)
                            {
                                *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);                                        
                                curlen = sdslen(*publishString);
                                if (first)
                                {
                                    publishString[0][curlen] = '\"';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);
                                    first = 0;
                                }
                                else{
                                    publishString[0][curlen] = ',';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);
                                }                                     
                                GetDataFromHash(orderobj, *getsetValueField, publishString);
                                
                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = '(';
                                publishString[0][curlen+1] = '\0';
                                sdssetlen(*publishString,curlen+1);

                                *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);                                                                     
                                GetDataFromHash88Replace(orderobj, *keyFieldsds, getsetValueField);
                                long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);    
                                if (amountLeft >= 1000)
                                {
                                    amountLeft = amountLeft / 1000;
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                    *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                }
                                else
                                {
                                    *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                }
                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = ')';
                                publishString[0][curlen+1] = '\"';
                                publishString[0][curlen+2] = '\0';
                                sdssetlen(*publishString,curlen+2);
                            }                                
                            ln2 = ln2->level[0].forward;
                        }
                    }                        
                }
                curlen = sdslen(*publishString);
                publishString[0][curlen] = ']';
                publishString[0][curlen+1] = '\0';
                sdssetlen(*publishString,curlen+1);
                
                if (llen == 0)
                {
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = '\0';
                    sdssetlen(*publishString,curlen+1); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = '\0';
                    sdssetlen(*bidaskString,curlen+1); 
                }
                else
                {
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = ',';
                    publishString[0][curlen+2] = '\0';
                    sdssetlen(*publishString,curlen+2); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = ',';
                    bidaskString[0][curlen+2] = '\0';
                    sdssetlen(*bidaskString,curlen+2); 
                }
                ln = ln->backward;
            }
        }
       
    }
    
    *bidaskString = sdscat8klen(*bidaskString, "],\"Ask\":[",9);
    *publishString = sdscat8klen(*publishString, "],\"Ask\":[",9);
    *keyFieldsds = sds88replace(*keyFieldsds,marketAsk, strlen(marketAsk));
    if ((askzobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && askzobj->type == OBJ_ZSET)
    {
        llen = zsetLength(askzobj);
        if (number < llen)
           llen = number;

        if (askzobj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = askzobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            eptr = ziplistIndex(zl, 0);
            sptr = ziplistNext(zl, eptr);

            while (llen--)
            {               
                ziplistGet(eptr, &vstr, &vlen, &vlong);
                if (vstr != NULL)
                {
                    strncpy(amountMarketPrice,(char *)vstr,vlen);
                    amountMarketPrice[vlen]='\0';

                    tempstrtodigitPtr = NULL;
                    strcpy(tempAmount,strtok_r(amountMarketPrice, marketAsk,&tempstrtodigitPtr));
			        strcpy(tempPrice,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));                    
                    
                    stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                    *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));
                    *publishString = sdscat8klen(*publishString, tempBidAskStr,strlen(tempBidAskStr));
                   
                    
                    *publishString = sdscat8klen(*publishString, ",\"UserId\":[",11);
                    char marketPrice[50];
                    stpcpy(stpcpy(marketPrice,marketAsk), tempPrice);
                    *keyFieldsds = sds88replace(*keyFieldsds,marketPrice, strlen(marketPrice));
                    robj *marketPricezobj;

                    int ttl;
                    if ((marketPricezobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && marketPricezobj->type == OBJ_ZSET)
                    {                           
                        ttl = zsetLength(marketPricezobj);
                        if (marketPricezobj->encoding == OBJ_ENCODING_ZIPLIST)
                        {
                            unsigned char *zl2 = marketPricezobj->ptr;
                            unsigned char *eptr2, *sptr2;
                            unsigned char *vstr2;
                            unsigned int vlen2;
                            long long vlong2;

                            int first = 1;
                            eptr2 = ziplistIndex(zl2, 0);
                            sptr2 = ziplistNext(zl2, eptr2);
                            
                            char tempOrderId[30];
                            while (ttl--)
                            {                                    
                                ziplistGet(eptr2, &vstr2, &vlen2, &vlong2);
                                if (vstr2 == NULL)
                                {
                                    sprintf(tempOrderId, "%lld", vlong2);
                                }
                                else
                                {
                                    strncpy(tempOrderId,(char *)vstr2,vlen2);
                                    tempOrderId[vlen2] ='\0';
                                }
                                *keyFieldsds = sds88replace(*keyFieldsds,tempOrderId, strlen(tempOrderId));                                    
                                robj *orderobj;
                                /*find out the ask object and check it exists or not*/
                                orderobj = lookupKeyWrite(c->db, keyFieldKey);
                                if (orderobj != NULL)
                                {
                                    /*get order and store to sds*/
                                    *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);
                                    curlen = sdslen(*publishString);  
                                    if (first)
                                    {
                                        publishString[0][curlen] = '\"';
                                        publishString[0][curlen+1] = '\0';
                                        sdssetlen(*publishString,curlen+1);
                                        first = 0;
                                    }
                                    else{
                                        publishString[0][curlen] = ',';
                                        publishString[0][curlen+1] = '\"';
                                        publishString[0][curlen+2] = '\0';
                                        sdssetlen(*publishString,curlen+2);
                                    }   
                                    GetDataFromHash(orderobj, *getsetValueField, publishString);

                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = '(';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);

                                    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);                                          
                                    GetDataFromHash88Replace(orderobj, *keyFieldsds,getsetValueField);
                                    long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);
                                    if (amountLeft >= 1000)
                                    {
                                        amountLeft = amountLeft / 1000;
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                        *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                    }
                                    else
                                    {
                                        *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                    }
                                    
                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = ')';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);
                                }
                                zzlNext(zl, &eptr2, &sptr2);
                            }
                        }
                        else if (marketPricezobj->encoding == OBJ_ENCODING_SKIPLIST)
                        {
                            zset *zs = marketPricezobj->ptr;
                            zskiplist *zsl = zs->zsl;
                            zskiplistNode *ln;
                            sds ele;
                            int first = 1;
                            ln = zsl->header->level[0].forward;
                            while (ttl--)
                            {
                                ele = ln->ele;
                                *keyFieldsds = sds88replace(*keyFieldsds,ele, sdslen(ele)); 
                                robj *orderobj;
                                orderobj = lookupKeyWrite(c->db, keyFieldKey);
                                
                                if (orderobj != NULL)
                                {
                                    *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);                                        
                                    curlen = sdslen(*publishString);   
                                    if (first)
                                    {
                                        publishString[0][curlen] = '\"';
                                        publishString[0][curlen+1] = '\0';
                                        sdssetlen(*publishString,curlen+1);
                                        first = 0;
                                    }
                                    else{
                                        publishString[0][curlen] = ',';
                                        publishString[0][curlen+1] = '\"';
                                        publishString[0][curlen+2] = '\0';
                                        sdssetlen(*publishString,curlen+2);
                                    }                                             
                                    GetDataFromHash(orderobj, *getsetValueField, publishString);

                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = '(';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);

                                    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10); 
                                                                    
                                    GetDataFromHash88Replace(orderobj, *keyFieldsds, getsetValueField);
                                    long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);                                        
                                    if (amountLeft >= 1000)
                                    {
                                        amountLeft = amountLeft / 1000;
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                        *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                    }
                                    else
                                    {
                                        *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                    }
                                    curlen = sdslen(*publishString);
                                    publishString[0][curlen] = ')';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);                                      
                                }                                    
                                ln = ln->level[0].forward;
                            }
                        }
                        
                    }     
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = ']';
                    publishString[0][curlen+1] = '\0';
                    sdssetlen(*publishString,curlen+1);                 
                    
                }
                
                zzlNext(zl, &eptr, &sptr);
                if (llen == 0)
                {
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = '\0';
                    sdssetlen(*publishString,curlen+1); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = '\0';
                    sdssetlen(*bidaskString,curlen+1); 
                }
                else
                {
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = ',';
                    publishString[0][curlen+2] = '\0';
                    sdssetlen(*publishString,curlen+2); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = ',';
                    bidaskString[0][curlen+2] = '\0';
                    sdssetlen(*bidaskString,curlen+2); 
                }
            }
        }
        else if (askzobj->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = askzobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            sds ele;

            /* Check if starting point is trivial, before doing log(N) lookup. */
            ln = zsl->header->level[0].forward;
            while (llen--)
            {               
                ele = ln->ele;
                curlen = sdslen(ele);
                strncpy(amountMarketPrice,ele,curlen);
                amountMarketPrice[curlen]='\0';

                tempstrtodigitPtr = NULL;
                strcpy(tempAmount,strtok_r(amountMarketPrice, marketAsk,&tempstrtodigitPtr));
			    strcpy(tempPrice,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));
                stpcpy(stpcpy(stpcpy(stpcpy(tempBidAskStr,"{\"Price\":"), tempPrice), ",\"Amount\":"), tempAmount);
                *bidaskString = sdscat8klen(*bidaskString, tempBidAskStr,strlen(tempBidAskStr));
                *publishString = sdscat8klen(*publishString, tempBidAskStr,strlen(tempBidAskStr));
                
                
                *publishString = sdscat8klen(*publishString, ",\"UserId\":[",11);
                char marketPrice [50];
                stpcpy(stpcpy(marketPrice,marketAsk), tempPrice);
                *keyFieldsds = sds88replace(*keyFieldsds,marketPrice, strlen(marketPrice));
                robj *marketPricezobj;

                int ttl;
                if ((marketPricezobj = lookupKeyRead(c->db, keyFieldKey)) != NULL && marketPricezobj->type != OBJ_ZSET)
                {                        
                    ttl = zsetLength(marketPricezobj);
                    if (marketPricezobj->encoding == OBJ_ENCODING_ZIPLIST)
                    {
                        unsigned char *zl = marketPricezobj->ptr;
                        unsigned char *eptr, *sptr;
                        unsigned char *vstr;
                        unsigned int vlen;
                        long long vlong;

                        int first = 1;
                        eptr = ziplistIndex(zl, 0);
                        sptr = ziplistNext(zl, eptr);
                        char tempOrderId[30];

                        while (ttl--)
                        {
                            
                            ziplistGet(eptr, &vstr, &vlen, &vlong);
                            if (vstr == NULL)
                            {
                                    sprintf(tempOrderId, "%lld", vlong);
                            }
                            else
                            {
                                strncpy(tempOrderId,(char *)vstr,vlen);
                                tempOrderId[vlen] ='\0';
                            }
                            *keyFieldsds = sds88replace(*keyFieldsds,tempOrderId, strlen(tempOrderId));  
                            robj *orderobj;
                            orderobj = lookupKeyWrite(c->db, keyFieldKey);
                            if (orderobj != NULL)
                            {
                                *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);
                                curlen = sdslen(*publishString);                                      
                                if (first)
                                {
                                    publishString[0][curlen] = '\"';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);
                                    first = 0;
                                }
                                else{
                                    publishString[0][curlen] = ',';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);
                                }                     
                                GetDataFromHash(orderobj, *getsetValueField, publishString);

                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = '(';
                                publishString[0][curlen+1] = '\0';
                                sdssetlen(*publishString,curlen+1);

                                *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);                                           
                                GetDataFromHash88Replace(orderobj, *keyFieldsds, getsetValueField);
                                long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);
                                if (amountLeft >= 1000)
                                {
                                    amountLeft = amountLeft / 1000;
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                    *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                }
                                else
                                {
                                    *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                }
                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = ')';
                                publishString[0][curlen+1] = '\"';
                                publishString[0][curlen+2] = '\0';
                                sdssetlen(*publishString,curlen+2);
                            } 
                            zzlNext(zl, &eptr, &sptr);
                        }
                    }
                    else if (marketPricezobj->encoding == OBJ_ENCODING_SKIPLIST)
                    {
                        zset *zs2 = marketPricezobj->ptr;
                        zskiplist *zsl2 = zs2->zsl;
                        zskiplistNode *ln2;
                        sds ele2;
                        int first = 1;
                        ln2 = zsl2->header->level[0].forward;
                        while (ttl--)
                        {

                            ele2 = ln2->ele;
                            *keyFieldsds = sds88replace(*keyFieldsds,ele2, sdslen(ele2)); 
                            robj *orderobj;
                            orderobj = lookupKeyWrite(c->db, keyFieldKey);
                            if (orderobj != NULL)
                            {
                                *getsetValueField = sds88replace(*getsetValueField,"User_Id",7);                                        
                                curlen = sdslen(*publishString);                                      
                                if (first)
                                {
                                    publishString[0][curlen] = '\"';
                                    publishString[0][curlen+1] = '\0';
                                    sdssetlen(*publishString,curlen+1);
                                    first = 0;
                                }
                                else{
                                    publishString[0][curlen] = ',';
                                    publishString[0][curlen+1] = '\"';
                                    publishString[0][curlen+2] = '\0';
                                    sdssetlen(*publishString,curlen+2);
                                }                                       
                                GetDataFromHash(orderobj, *getsetValueField, publishString);
                                
                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = '(';
                                publishString[0][curlen+1] = '\0';
                                sdssetlen(*publishString,curlen+1);


                                *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft",10);                                                                         
                                GetDataFromHash88Replace(orderobj, *keyFieldsds, getsetValueField);
                                long double amountLeft = strtold(*getsetValueField,&tempstrtodigitPtr);    
                                if (amountLeft >= 1000)
                                {
                                    amountLeft = amountLeft / 1000;
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,"%.2LF k", amountLeft);
                                    *publishString = sdscat8klen(*publishString,tempAmountLeft,strlen(tempAmountLeft));
                                }
                                else
                                {
                                    *publishString = sdscat8klen(*publishString, *getsetValueField,sdslen(*getsetValueField));
                                }
                                curlen = sdslen(*publishString);
                                publishString[0][curlen] = ')';
                                publishString[0][curlen+1] = '\"';
                                publishString[0][curlen+2] = '\0';
                                sdssetlen(*publishString,curlen+2);                                      
                            }                                
                            ln2 = ln2->level[0].forward;
                        }
                    }
                    
                }
                curlen = sdslen(*publishString);
                publishString[0][curlen] = ']';
                publishString[0][curlen+1] = '\0';
                sdssetlen(*publishString,curlen+1); 
                    
                               
                ln = ln->level[0].forward;
                if (llen == 0)
                {
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = '\0';
                    sdssetlen(*publishString,curlen+1); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = '\0';
                    sdssetlen(*bidaskString,curlen+1); 
                }
                else
                {
                    curlen = sdslen(*publishString);
                    publishString[0][curlen] = '}';
                    publishString[0][curlen+1] = ',';
                    publishString[0][curlen+2] = '\0';
                    sdssetlen(*publishString,curlen+2); 


                    curlen = sdslen(*bidaskString);
                    bidaskString[0][curlen] = '}';
                    bidaskString[0][curlen+1] = ',';
                    bidaskString[0][curlen+2] = '\0';
                    sdssetlen(*bidaskString,curlen+2);
                }
            }
        }        
    }
    curlen = sdslen(*publishString);
    publishString[0][curlen] = ']';
    publishString[0][curlen+1] = '}';
    publishString[0][curlen+2] = '\0';
    sdssetlen(*publishString,curlen+2); 


    curlen = sdslen(*bidaskString);
    bidaskString[0][curlen] = ']';
    bidaskString[0][curlen+1] = '}';
    bidaskString[0][curlen+2] = '\0';
    sdssetlen(*bidaskString,curlen+2);

}
void GenerateOutputJson(client *c, sds *OrderExecutedStrings,  sds *OrderPriceExecutedStrings, char *orderId,long double averageExecutedPrice,long double totalAmountExecuted,sds *keyFieldsds,robj *keyFieldKey)
{
    int totalOutput = 4;

    char outputPriceExecutedString[sdslen(*OrderPriceExecutedStrings)+25];
    if(sdslen(*OrderPriceExecutedStrings)>0){
        char tempRobotChannel[40];
        stpcpy(stpcpy(tempRobotChannel,"robot_xyz_"),c->argv[1]->ptr);

        stpcpy(stpcpy(stpcpy(outputPriceExecutedString,"{\"PriceExecuted\":["),*OrderPriceExecutedStrings+1),"]}"); 
        *OrderPriceExecutedStrings = sds8kreplace(*OrderPriceExecutedStrings,outputPriceExecutedString,sdslen(outputPriceExecutedString)+20);
        robj *messageValueKey = createObject(OBJ_STRING,*OrderPriceExecutedStrings);
        *keyFieldsds = sds88replace(*keyFieldsds,tempRobotChannel,strlen(tempRobotChannel));            
        pubsubPublishMessage(keyFieldKey,messageValueKey);    
        messageValueKey->ptr = NULL;
        zfree(messageValueKey);
    }
    else{
        stpcpy(outputPriceExecutedString,"{\"PriceExecuted\":[]}");
    }   
   
    char tempPrintfStr[20];
    char ouptputAveragePriceAmount[50];
    stpcpy(stpcpy(stpcpy(tempPrintfStr,c->argv[3]->ptr)," "),c->argv[4]->ptr);
    sprintf(ouptputAveragePriceAmount,tempPrintfStr,averageExecutedPrice,totalAmountExecuted);  
  
    
    addReplyMultiBulkLen(c, totalOutput);
    addReplyBulkCBuffer(c, orderId, strlen(orderId));
    if(sdslen(*OrderExecutedStrings)>0){
        addReplyBulkCBuffer(c, *OrderExecutedStrings+1, sdslen(*OrderExecutedStrings)-1);
    }
    else{
        addReplyBulkCBuffer(c, *OrderExecutedStrings, sdslen(*OrderExecutedStrings));
    }
    addReplyBulkCBuffer(c, outputPriceExecutedString,strlen(outputPriceExecutedString) );
    addReplyBulkCBuffer(c, ouptputAveragePriceAmount, strlen(ouptputAveragePriceAmount));
      
  
    
}
void GenerateMarketOutputJson(client *c, char *orderId, long double moneyRemain, sds *OrderExecutedStrings,  sds *OrderPriceExecutedStrings,long double averageExecutedPrice,long double totalAmountExecuted,sds *keyFieldsds,robj *keyFieldKey)
{
   
    int totalOutput = 5;

    char outputPriceExecutedString[sdslen(*OrderPriceExecutedStrings)+25];
    if(sdslen(*OrderPriceExecutedStrings)>0){
        char tempRobotChannel[40];
        stpcpy(stpcpy(tempRobotChannel,"robot_xyz_"),c->argv[1]->ptr);

        stpcpy(stpcpy(stpcpy(outputPriceExecutedString,"{\"PriceExecuted\":["),*OrderPriceExecutedStrings+1),"]}");
        *OrderPriceExecutedStrings = sds8kreplace(*OrderPriceExecutedStrings,outputPriceExecutedString,sdslen(outputPriceExecutedString)+20);
        robj *messageValueKey = createObject(OBJ_STRING,*OrderPriceExecutedStrings);
        *keyFieldsds = sds88replace(*keyFieldsds,tempRobotChannel,strlen(tempRobotChannel));            
        pubsubPublishMessage(keyFieldKey,messageValueKey);    
        messageValueKey->ptr = NULL;
        zfree(messageValueKey);
    }
    else{
        stpcpy(outputPriceExecutedString,"{\"PriceExecuted\":[]}");
    }
   
    char tempMoneyRemainAmount[30];
    sprintf(tempMoneyRemainAmount,c->argv[8]->ptr,moneyRemain);

    char tempPrintfStr[20];
    char ouptputAveragePriceAmount[50];
    stpcpy(stpcpy(stpcpy(tempPrintfStr,c->argv[4]->ptr)," "),c->argv[6]->ptr);
    sprintf(ouptputAveragePriceAmount,tempPrintfStr,averageExecutedPrice,totalAmountExecuted);

    addReplyMultiBulkLen(c, totalOutput);
    addReplyBulkCBuffer(c, orderId, strlen(orderId));
    addReplyBulkCBuffer(c, tempMoneyRemainAmount, strlen(tempMoneyRemainAmount));
    if(sdslen(*OrderExecutedStrings)>0){
        addReplyBulkCBuffer(c, *OrderExecutedStrings+1, sdslen(*OrderExecutedStrings)-1);
    }
    else{
        addReplyBulkCBuffer(c, *OrderExecutedStrings, sdslen(*OrderExecutedStrings));
    }
    addReplyBulkCBuffer(c, outputPriceExecutedString, strlen(outputPriceExecutedString));
    addReplyBulkCBuffer(c, ouptputAveragePriceAmount, strlen(ouptputAveragePriceAmount));    
   
}
void AddOrderToSelfSide(client *c, char *myOrderId,long double orderIdLongDouble, char *orderBuySell, char *side,long double orderPrice ,long double CurrentAmount,sds *keyFieldsds , robj *keyFieldKey, sds *getsetValueField)
{
    char marketPrice[40];
    stpcpy(stpcpy(marketPrice,side),c->argv[5]->ptr);
    *getsetValueField = sds88replace(*getsetValueField,myOrderId,strlen(myOrderId));

    *keyFieldsds = sds88replace(*keyFieldsds,marketPrice,strlen(marketPrice));    
    robj *zobj = lookupKeyWrite(c->db, keyFieldKey);

    singleZAddWithKeyObj(c,keyFieldKey,zobj, *getsetValueField, orderIdLongDouble);
    AddOrderHashRecord(c, myOrderId, orderBuySell, CurrentAmount,keyFieldsds,keyFieldKey,getsetValueField);
   
    *keyFieldsds = sds88replace(*keyFieldsds,side,strlen(side));    
    zobj = lookupKeyWrite(c->db, keyFieldKey);
    if (zobj == NULL)
    {        
        char tempamountMarketPrice[70];
        char tempPrintfStr[60];
        stpcpy(stpcpy(tempPrintfStr,c->argv[4]->ptr),marketPrice);
        sprintf(tempamountMarketPrice,tempPrintfStr,CurrentAmount);

        *getsetValueField = sds88replace(*getsetValueField,tempamountMarketPrice,strlen(tempamountMarketPrice));
        singleZAddWithKeyObj(c,keyFieldKey,zobj,*getsetValueField,orderPrice);
    }
    else
    {
        zrangespec range = {.min = orderPrice, .max = orderPrice, .minex = 0, .maxex = 0};
        char *tempstrtodigitPtr; 
        if (zobj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = zobj->ptr;
            unsigned char *eptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            eptr = zzlFirstInRange(zl, &range);

            if (eptr != NULL)
            {
                ziplistGet(eptr, &vstr, &vlen, &vlong);
                if (vstr != NULL)
                {
                    *getsetValueField = sds88replace(*getsetValueField,vstr, vlen);
                    long double storedAmount = strtold(*getsetValueField,&tempstrtodigitPtr);
                    CurrentAmount +=  storedAmount;

                }
                else
                {
                    char tempoldAmountMarketPrice[30];
                    sprintf( tempoldAmountMarketPrice,"%lld", vlong);
                    *getsetValueField = sds88replace(*getsetValueField,tempoldAmountMarketPrice, strlen(tempoldAmountMarketPrice));                   
                    CurrentAmount +=  vlong;
                }
            }
        }
        else if (zobj->encoding == OBJ_ENCODING_SKIPLIST)
        {
            zset *zs = zobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            ln = zslFirstInRange(zsl, &range);
            if (ln != NULL)
            {
                *getsetValueField = sds88replace(*getsetValueField,ln->ele, sdslen(ln->ele));
                long double storedAmount = strtold(*getsetValueField,&tempstrtodigitPtr);
                CurrentAmount +=  storedAmount;
            }
        }
        zsetDel(zobj, *getsetValueField);
        
        char tempamountMarketPrice[70];
        char tempPrintfStr[60];
        stpcpy(stpcpy(tempPrintfStr,c->argv[4]->ptr),marketPrice);
        sprintf(tempamountMarketPrice,tempPrintfStr,CurrentAmount);
        *getsetValueField = sds88replace(*getsetValueField,tempamountMarketPrice, strlen(tempamountMarketPrice));
        singleZAddWithKeyObj(c, keyFieldKey,zobj, *getsetValueField, orderPrice);                
    }    
}
long double ExecuteStatedPriceOrders(client *c, char *theKey, long double amount, sds *orderExecutedString,sds *keyFieldsds , robj *keyFieldKey, sds *getsetValueField)
{   
    size_t theKeyLen = strlen(theKey);
    long double resultAmountLeft = amount;
    robj *orderListobj;
    int llen;

    *keyFieldsds = sds88replace(*keyFieldsds,theKey,theKeyLen);
    orderListobj = lookupKeyWrite(c->db, keyFieldKey);   
    if (orderListobj == NULL || (orderListobj->type != OBJ_ZSET))
    {
        return resultAmountLeft;
    }
    else
    {
        llen = zsetLength(orderListobj);
        char listOfOrderNeedToDelete[llen][30];
        int orderDeleteCounter = 0;
        char orderId[30];
        char *tempstrtodigitPtr;
        size_t orderIdLen;
        if (orderListobj->encoding == OBJ_ENCODING_ZIPLIST)
        {
            unsigned char *zl = orderListobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;
            eptr = ziplistIndex(zl, 0);
            sptr = ziplistNext(zl, eptr);

            while (llen-- && resultAmountLeft > 0)
            {                
                ziplistGet(eptr, &vstr, &vlen, &vlong);
                if (vstr == NULL){
                    sprintf(orderId, "%lld", vlong);
                }
                else{
                    strncpy(orderId,(char *)vstr,vlen);
                    orderId[vlen] ='\0';
                }
                orderIdLen =strlen(orderId);
                *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                robj *orderObject = lookupKeyWrite(c->db, keyFieldKey);
                *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft", 10);
                GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);
                long double orderLastRemain = strtold(*getsetValueField,&tempstrtodigitPtr);
                long double orderAmountLeft = orderLastRemain - resultAmountLeft;
                long double orderExecutedAmount = orderLastRemain;
                if (orderAmountLeft <= 0)
                {
                    *keyFieldsds = sds88replace(*keyFieldsds,"User_Id", 7); 
                    GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);

                    char tempResultStr[70];         
                    char tempExecutedAmount[30];
                    sprintf(tempExecutedAmount,c->argv[4]->ptr,orderExecutedAmount);
                    stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempResultStr," "),*getsetValueField)," "),orderId)," "),tempExecutedAmount);           
                    
                    *orderExecutedString = sdscat8klen(*orderExecutedString,tempResultStr,strlen(tempResultStr));

                    *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                    dbDelete(c->db, keyFieldKey);

                    stpcpy(listOfOrderNeedToDelete[orderDeleteCounter] ,orderId);
                    orderDeleteCounter++;           
                }
                else{
                    orderExecutedAmount -= orderAmountLeft;

                    if (orderObject != NULL && orderObject->type == OBJ_HASH)
                    {
                        int created = 0;
                        char tempLeft[30];
                        sprintf(tempLeft, c->argv[4]->ptr,orderAmountLeft);   
                        *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                        created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);

                        sprintf(tempLeft, c->argv[4]->ptr,orderLastRemain);
                        *keyFieldsds = sds88replace(*keyFieldsds,"AmountLastLeft", 14);
                        *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                        created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);
                    
                        if (created > 0)
                        {
                            *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                            signalModifiedKey(c->db, keyFieldKey);
                            server.dirty++;
                        }

                        *keyFieldsds = sds88replace(*keyFieldsds,"User_Id", 7); 
                        GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);

                        char tempResultStr[70];         
                        char tempExecutedAmount[30];
                        sprintf(tempExecutedAmount,c->argv[4]->ptr,orderExecutedAmount);
                        stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempResultStr," "),*getsetValueField)," "),orderId)," "),tempExecutedAmount);           
                        
                        *orderExecutedString = sdscat8klen(*orderExecutedString,tempResultStr,strlen(tempResultStr));
                    }   
                }           
                
                resultAmountLeft = resultAmountLeft - orderLastRemain;

                zzlNext(zl, &eptr, &sptr);
            }
        }
        else if (orderListobj->encoding == OBJ_ENCODING_SKIPLIST)
        {           
            zset *zs = orderListobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *ln;
            

            ln = zsl->header->level[0].forward;
            while (llen-- && resultAmountLeft > 0)
            {

                if (ln != NULL)
                {
                    strcpy(orderId, ln->ele);
                    orderIdLen =strlen(orderId);
                    *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                    robj *orderObject = lookupKeyWrite(c->db, keyFieldKey);

                    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft", 10);
                    GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);
                    long double orderLastRemain = strtold(*getsetValueField,&tempstrtodigitPtr);
                    long double orderAmountLeft = orderLastRemain - resultAmountLeft;
                    long double orderExecutedAmount = orderLastRemain;
                    if (orderAmountLeft <= 0)
                    {
                        *keyFieldsds = sds88replace(*keyFieldsds,"User_Id", 7); 
                        GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);                       

                        char tempResultStr[70];         
                        char tempExecutedAmount[30];
                        sprintf(tempExecutedAmount,c->argv[4]->ptr,orderExecutedAmount);
                        stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempResultStr," "),*getsetValueField)," "),orderId)," "),tempExecutedAmount);           
                        
                        *orderExecutedString = sdscat8klen(*orderExecutedString,tempResultStr,strlen(tempResultStr));

                        *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                        dbDelete(c->db, keyFieldKey);
                        signalModifiedKey(c->db,keyFieldKey);
                        stpcpy(listOfOrderNeedToDelete[orderDeleteCounter] ,orderId);
                        orderDeleteCounter++;
                    }
                    else{
                        orderExecutedAmount -= orderAmountLeft;
                    
                        if (orderObject != NULL && orderObject->type == OBJ_HASH)
                        {
                            int created = 0;
                            char tempLeft[30];
                            sprintf(tempLeft, c->argv[4]->ptr,orderAmountLeft);                        
                            *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                            created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);
                        
                            sprintf(tempLeft, c->argv[4]->ptr,orderLastRemain);
                            *keyFieldsds = sds88replace(*keyFieldsds,"AmountLastLeft", 14);
                            *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                            created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);
                    
                            if (created > 0)
                            {
                                *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                                signalModifiedKey(c->db, keyFieldKey);
                                server.dirty++;
                            }
                            *keyFieldsds = sds88replace(*keyFieldsds,"User_Id", 7); 
                            GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);                       

                            char tempResultStr[70];         
                            char tempExecutedAmount[30];
                            sprintf(tempExecutedAmount,c->argv[4]->ptr,orderExecutedAmount);
                            stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempResultStr," "),*getsetValueField)," "),orderId)," "),tempExecutedAmount);           
                            
                            *orderExecutedString = sdscat8klen(*orderExecutedString,tempResultStr,strlen(tempResultStr));
                        
                        } 
                    }                  
                    resultAmountLeft = resultAmountLeft - orderLastRemain;
                    
                }
                ln = ln->level[0].forward;
            }
        }
        int delete = 0;
        *keyFieldsds = sds88replace(*keyFieldsds,theKey,theKeyLen);

        if(orderDeleteCounter>0){
            
            for (int i = 0; i < orderDeleteCounter; i++)
            {
                *getsetValueField = sds88replace(*getsetValueField,listOfOrderNeedToDelete[i],strlen(listOfOrderNeedToDelete[i]));
                if (zsetDel(orderListobj,  *getsetValueField)){
                    delete = 1;
                }
                if (zsetLength(orderListobj) == 0)
                {
                    dbDelete(c->db, keyFieldKey);               
                }
            }
        }
        if (delete)
        {
            signalModifiedKey(c->db, keyFieldKey);
            server.dirty += delete;
        }       
    }    
    return resultAmountLeft;
}

long double ExecuteStatedMarketPriceOrders(client *c, char *theKey, long double *amountWantToPlace, long double *money, long double marginRate,  sds *orderExecutedString,long long amountDecimalPow, long double currentBidAskAmount, long double currentBidAskPrice, char *isContractCash, double contractMultiplier,sds *keyFieldsds , robj *keyFieldKey, sds *getsetValueField)
{
    int llen;
    robj *orderListobj;

   
    long double moneyRemain = *money;
    long double buyingPower = moneyRemain * marginRate;
    long double totalOrderAmountPriceValue;
    if (isContractCash[0] == '1')
    {
        totalOrderAmountPriceValue = currentBidAskAmount * contractMultiplier / currentBidAskPrice;
    }
    else
    {
        totalOrderAmountPriceValue = currentBidAskAmount * currentBidAskPrice;
    }

    long double amountCanBuy = 0;

    long double totalAmountWantToPlaceRemain = *amountWantToPlace;
    long double totalMarketSidePriceAmountRemain = 0;

    if (buyingPower >= totalOrderAmountPriceValue)
    {
        amountCanBuy = currentBidAskAmount;
        if (totalAmountWantToPlaceRemain < amountCanBuy)
        {
            amountCanBuy = totalAmountWantToPlaceRemain;
            totalAmountWantToPlaceRemain = 0;
            totalMarketSidePriceAmountRemain = currentBidAskAmount - amountCanBuy;
        }
        else
        {
            totalMarketSidePriceAmountRemain = 0;
            totalAmountWantToPlaceRemain = totalAmountWantToPlaceRemain - currentBidAskAmount;
        }
    }
    else
    {
        if (isContractCash[0] == '1')
        {
            amountCanBuy = floorl((currentBidAskPrice * buyingPower) * amountDecimalPow) / amountDecimalPow;
        }
        else
        {
            amountCanBuy = floorl((buyingPower / currentBidAskPrice) * amountDecimalPow) / amountDecimalPow;
        }
       

        if (totalAmountWantToPlaceRemain < amountCanBuy)
        {
            amountCanBuy = totalAmountWantToPlaceRemain;
            totalAmountWantToPlaceRemain = 0;
        }
        else
        {
            totalAmountWantToPlaceRemain = (totalAmountWantToPlaceRemain - amountCanBuy) * -1;
        }

        totalMarketSidePriceAmountRemain = currentBidAskAmount - amountCanBuy;
    }
    if (isContractCash[0] == '1')
    {        
        moneyRemain = (buyingPower - (amountCanBuy / currentBidAskPrice)) / marginRate;
    }
    else
    {
        moneyRemain = (buyingPower - (currentBidAskPrice * amountCanBuy)) / marginRate;
    }
	
    *money = moneyRemain;
    *amountWantToPlace = totalAmountWantToPlaceRemain;

    if (amountCanBuy == 0)
    {        
        return totalMarketSidePriceAmountRemain;
    }
    else
    {
        size_t theKeyLen = strlen(theKey);
        *keyFieldsds = sds88replace(*keyFieldsds,theKey,theKeyLen);
        orderListobj = lookupKeyWrite(c->db, keyFieldKey);
        if (orderListobj == NULL || (orderListobj->type != OBJ_ZSET))
        {            
            return totalMarketSidePriceAmountRemain;
        }
        else
        {
            llen = zsetLength(orderListobj);
            char listOfOrderNeedToDelete[llen][30];
            int orderDeleteCounter = 0;
            char orderId[30];
            char *tempstrtodigitPtr;
            size_t orderIdLen;

            if (orderListobj->encoding == OBJ_ENCODING_ZIPLIST)
            {
                unsigned char *zl = orderListobj->ptr;
                unsigned char *eptr, *sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;
                eptr = ziplistIndex(zl, 0);
                sptr = ziplistNext(zl, eptr);

                while (llen-- && amountCanBuy > 0)
                {                    
                    ziplistGet(eptr, &vstr, &vlen, &vlong);
                    if (vstr == NULL){
                        sprintf(orderId, "%lld", vlong);
                    }
                    else{
                        strncpy(orderId,(char *)vstr,vlen);
                        orderId[vlen] ='\0';
                    }
                    orderIdLen =strlen(orderId);
                    *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                    robj *orderObject = lookupKeyWrite(c->db, keyFieldKey);
                    *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft", 10);
                    GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);
                    long double orderLastRemain = strtold(*getsetValueField,&tempstrtodigitPtr);
                    long double orderAmountLeft = orderLastRemain - amountCanBuy;
                    long double orderExecutedAmount = orderLastRemain;
                    if (orderAmountLeft <= 0)
                    {
                        *keyFieldsds = sds88replace(*keyFieldsds,"User_Id", 7); 
                        GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);                       

                        char tempResultStr[80];         
                        char tempExecutedAmount[40];
                        sprintf(tempExecutedAmount,c->argv[4]->ptr,orderExecutedAmount);
                        stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempResultStr," "),*getsetValueField)," "),orderId)," "),tempExecutedAmount);           
                        
                        *orderExecutedString = sdscat8klen(*orderExecutedString,tempResultStr,strlen(tempResultStr));
                        

                        *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                        dbDelete(c->db, keyFieldKey);

                        stpcpy(listOfOrderNeedToDelete[orderDeleteCounter] ,orderId);
                        orderDeleteCounter++;   
                    }
                    else{
                        orderExecutedAmount -= orderAmountLeft;
                        if (orderObject != NULL && orderObject->type == OBJ_HASH)
                        {
                            
                            int created = 0;
                            char tempLeft[30];
                            sprintf(tempLeft, c->argv[4]->ptr,orderAmountLeft);   
                            *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                            created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);

                            sprintf(tempLeft, c->argv[4]->ptr,orderLastRemain);
                            *keyFieldsds = sds88replace(*keyFieldsds,"AmountLastLeft", 14);
                            *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                            created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);
                                                  
                            if (created > 0)
                            {
                                *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                                signalModifiedKey(c->db, keyFieldKey);
                                server.dirty++;
                            }
                            *keyFieldsds = sds88replace(*keyFieldsds,"User_Id", 7); 
                            GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);

                            char tempResultStr[70];         
                            char tempExecutedAmount[30];
                            sprintf(tempExecutedAmount,c->argv[6]->ptr,orderExecutedAmount);
                            stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempResultStr," "),*getsetValueField)," "),orderId)," "),tempExecutedAmount);           
                            
                            *orderExecutedString = sdscat8klen(*orderExecutedString,tempResultStr,strlen(tempResultStr));
                        }    
                    }
                    amountCanBuy = amountCanBuy - orderLastRemain;

                    zzlNext(zl, &eptr, &sptr);
                }
            }
            else if (orderListobj->encoding == OBJ_ENCODING_SKIPLIST)
            {
                zset *zs = orderListobj->ptr;
                zskiplist *zsl = zs->zsl;
                zskiplistNode *ln;
               
                ln = zsl->header->level[0].forward;
                while (llen-- && amountCanBuy > 0)
                {
                    if (ln != NULL)
                    {
                        strcpy(orderId, ln->ele);
                        orderIdLen =strlen(orderId);

                        *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                        robj *orderObject = lookupKeyWrite(c->db, keyFieldKey);

                        *keyFieldsds = sds88replace(*keyFieldsds,"AmountLeft", 10);
                        GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);

                        long double orderLastRemain = strtold(*getsetValueField,&tempstrtodigitPtr);
                        long double orderAmountLeft = orderLastRemain - amountCanBuy;
                        long double orderExecutedAmount = orderLastRemain;
                        if (orderAmountLeft <= 0)
                        {
                            *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                            dbDelete(c->db, keyFieldKey);

                            stpcpy(listOfOrderNeedToDelete[orderDeleteCounter] ,orderId);
                            orderDeleteCounter++;
                        }
                        else{

                            orderExecutedAmount-= orderAmountLeft;
                        
                            if (orderObject != NULL && orderObject->type == OBJ_HASH)
                            {
                                int created = 0;
                                char tempLeft[30];
                                sprintf(tempLeft, c->argv[4]->ptr,orderAmountLeft);                        
                                *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                                created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);
                            
                                sprintf(tempLeft, c->argv[4]->ptr,orderLastRemain);
                                *keyFieldsds = sds88replace(*keyFieldsds,"AmountLastLeft", 14);
                                *getsetValueField = sds88replace(*getsetValueField,tempLeft,strlen(tempLeft));
                                created += !hashTypeSet(orderObject, *keyFieldsds, *getsetValueField , HASH_SET_COPY);
                    
                                if (created > 0)
                                {
                                    *keyFieldsds = sds88replace(*keyFieldsds,orderId, orderIdLen);
                                    signalModifiedKey(c->db, keyFieldKey);
                                    server.dirty++;
                                }
                                *keyFieldsds = sds88replace(*keyFieldsds,"User_Id", 7); 
                                GetDataFromHash88Replace(orderObject,*keyFieldsds,getsetValueField);                       


                                char tempResultStr[70];         
                                char tempExecutedAmount[30];
                                sprintf(tempExecutedAmount,c->argv[6]->ptr,orderExecutedAmount);
                                stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempResultStr," "),*getsetValueField)," "),orderId)," "),tempExecutedAmount);           
                                
                                *orderExecutedString = sdscat8klen(*orderExecutedString,tempResultStr,strlen(tempResultStr));

                            }   
                        }                        
                        amountCanBuy = amountCanBuy - orderLastRemain;                                                
                    }
                    ln = ln->level[0].forward;
                }            
			}
            int delete = 0;
            *keyFieldsds = sds88replace(*keyFieldsds,theKey,theKeyLen);

            if(orderDeleteCounter>0){
               
                for (int i = 0; i < orderDeleteCounter; i++)
                {
                   *getsetValueField = sds88replace(*getsetValueField,listOfOrderNeedToDelete[i],strlen(listOfOrderNeedToDelete[i]));
                    if (zsetDel(orderListobj,  *getsetValueField)){
                        delete = 1;
                    }
                    if (zsetLength(orderListobj) == 0)
                    {
                        dbDelete(c->db, keyFieldKey);               
                    }
                }                
            }
            if (delete)
            {               
                signalModifiedKey(c->db, keyFieldKey);
                server.dirty += delete;
            }
            
        }      
        
        return totalMarketSidePriceAmountRemain;
    }
}
void PlaceBuyLimitedOrderCommand(client *c)
{
    /*
    argv 1 = market
    argv 2 = userId     
    argv 3 = price decimal format 
    argv 4 = amount decimal format 
    argv 5 = price
    argv 6 = amount
    */
    
   
    char *buySell = "1";
    char orderId[30] ;
    char *tempstrtodigitPtr; 

    long double price = strtold(c->argv[5]->ptr,&tempstrtodigitPtr); 
    long double amount = strtold(c->argv[6]->ptr,&tempstrtodigitPtr); 

    long double averageExecutedPrice = 0;
    long double executedPriceAmount = 0;
    long double totalAmountExecuted = 0;
   
    sds executedOrderStrings = sdsempty8k();
    sds executedPriceAmountStrings = sdsempty8k();

    char marketBid[20];
    char marketAsk[20];   
    stpcpy(stpcpy(marketBid,c->argv[1]->ptr),"BID");
    stpcpy(stpcpy(marketAsk,c->argv[1]->ptr),"ASK");

    long long orderIdLongLong = getCurrentTimeStampLongLong();  
    sprintf(orderId, "%lld", orderIdLongLong); 
    
    size_t marketAskLen =  strlen(marketAsk);
    sds keyFieldsds = sdsnew88len(marketAsk,marketAskLen);
    robj *keyFieldKey = createObject(OBJ_STRING,keyFieldsds);
    sds getsetValueField = sdsempty88();
   
    robj *askrobj;
    
    askrobj = lookupKeyWrite(c->db, keyFieldKey);
    if (askrobj == NULL)
    {
        
        AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketBid,price, amount,&keyFieldsds,keyFieldKey,&getsetValueField);
       
        GenerateOutputJson(c, &executedOrderStrings, &executedPriceAmountStrings, orderId,averageExecutedPrice,totalAmountExecuted,&keyFieldsds,keyFieldKey);       
    }
    else
    {
        if (askrobj->type != OBJ_ZSET)
        {
            int deleted = dbSyncDelete(c->db, keyFieldKey);
            if (deleted)
            {
                signalModifiedKey(c->db, keyFieldKey);
                server.dirty++;
                
                AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketBid,price, amount,&keyFieldsds,keyFieldKey,&getsetValueField);
                
                GenerateOutputJson(c, &executedOrderStrings, &executedPriceAmountStrings, orderId, averageExecutedPrice,totalAmountExecuted,&keyFieldsds,keyFieldKey);
            }
        }
        else
        {
            int llen = zsetLength(askrobj);
            long double amountRemain = amount;
            char listToDelete[llen][80]; 
            char amountMarketPriceAddBack[80];
            char currentAmountMarketPrice[80];
            char currentAmount[30];
            char currentPriceStr[30];
            
            long double addBackPrice = 0; 
            int deleteCounter = 0;
            int addBack = 0;
            
            if (askrobj->encoding == OBJ_ENCODING_ZIPLIST)
            {
                unsigned char *zl = askrobj->ptr;
                unsigned char *eptr, *sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;

                eptr = ziplistIndex(zl, 0);

                sptr = ziplistNext(zl, eptr);
                while (llen-- && amountRemain > 0)
                {
                    long double currentOrderListAmount = 0;
                    long double currentPrice;

                    ziplistGet(eptr, &vstr, &vlen, &vlong);
                    if (vstr == NULL){
                       sprintf(currentAmountMarketPrice, "%lld", vlong);
                    }
                    else{
                        strncpy(currentAmountMarketPrice,(char *)vstr, vlen);
                        currentAmountMarketPrice[vlen]='\0';
                    }
                   
                    currentPrice = zzlGetScore(sptr);
                    
                    
                    if (price < currentPrice)
                    {
                        AddOrderToSelfSide(c, orderId, orderIdLongLong,buySell, marketBid,price, amountRemain,&keyFieldsds,keyFieldKey,&getsetValueField);                       
                        break;
                    }
                    else
                    {
                        strcpy(listToDelete[deleteCounter],currentAmountMarketPrice);
                        ++deleteCounter;
                        tempstrtodigitPtr = NULL;
                        strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketAsk,&tempstrtodigitPtr));
                        strcpy(currentPriceStr,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));    
                        tempstrtodigitPtr = NULL;    
                        currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr); 

                        char currentMarketSidePrice[50];
                        stpcpy(stpcpy(currentMarketSidePrice, marketAsk), currentPriceStr);                        
                        long double amountLeftAfterExecuteOrder = ExecuteStatedPriceOrders(c, currentMarketSidePrice, amountRemain, &executedOrderStrings,&keyFieldsds,keyFieldKey,&getsetValueField);
                        long double amountExecuted;
                        if (amountLeftAfterExecuteOrder > 0)
                        {
                            amountExecuted = currentOrderListAmount;
                        }
                        else
                        {
                            amountExecuted = amountRemain;
                        }
                        totalAmountExecuted += amountExecuted;
                        executedPriceAmount += (amountExecuted *  currentPrice);

                        char tempExecutedPriceAmountString[80];
                        char tempAmountExecutedStr[30];
                        sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                        stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                        executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));
                       
                        

                        if (price == currentPrice)
                        {
                            if (amountLeftAfterExecuteOrder > 0)
                            {
                                AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketBid,price,  amountLeftAfterExecuteOrder,&keyFieldsds,keyFieldKey,&getsetValueField);
                            }
                            else
                            {                               
                                long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                if (orginalOrdersAmountLeft > 0)
                                {
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                    stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                    addBackPrice = currentPrice;
                                    addBack = 1;
                                }
                            }                            
                            break;
                        }
                        else
                        {
                            if (amountLeftAfterExecuteOrder <= 0)
                            {
                                long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                if (orginalOrdersAmountLeft > 0)
                                {
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                    stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                    addBack = 1;
                                    addBackPrice = currentPrice;
                                }                                
                                break;
                            }
                        }
                        amountRemain = amountLeftAfterExecuteOrder;
                    }
                    if (llen == 0)
                    {
                        if (amountRemain > 0 && price > currentPrice)
                        {
                            AddOrderToSelfSide(c, orderId,orderIdLongLong ,buySell, marketBid,price, amountRemain,&keyFieldsds,keyFieldKey,&getsetValueField);                            
                        }
                    }

                    zzlNext(zl, &eptr, &sptr);
                }
            }
            else if (askrobj->encoding == OBJ_ENCODING_SKIPLIST)
            {
                zset *zs = askrobj->ptr;
                zskiplist *zsl = zs->zsl;
                zskiplistNode *ln;

                ln = zsl->header->level[0].forward;

                while (llen-- && amountRemain > 0)
                {
                    if (ln != NULL)
                    {
                        strcpy(currentAmountMarketPrice,ln->ele);                       
                        long double currentOrderListAmount = 0;
                        long double currentPrice;

                        currentPrice = ln->score;
                       

                        if (price < currentPrice)
                        {
                            AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketBid,price, amountRemain,&keyFieldsds,keyFieldKey,&getsetValueField); 
                            break;
                        }
                        else
                        {
                            tempstrtodigitPtr = NULL;
                            strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketAsk,&tempstrtodigitPtr));
                            strcpy(currentPriceStr,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));    
                            tempstrtodigitPtr = NULL;    
                            currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr); 

                            char currentMarketSidePrice[50];
                            stpcpy(stpcpy(currentMarketSidePrice, marketAsk), currentPriceStr);                        
                            
                            long double amountLeftAfterExecuteOrder = ExecuteStatedPriceOrders(c, currentMarketSidePrice, amountRemain, &executedOrderStrings,&keyFieldsds,keyFieldKey,&getsetValueField);
                            long double amountExecuted;
                            if (amountLeftAfterExecuteOrder > 0)
                            {
                                amountExecuted = currentOrderListAmount;
                            }
                            else
                            {
                                amountExecuted = amountRemain;
                            }
                            totalAmountExecuted += amountExecuted;
                            executedPriceAmount += (amountExecuted *  currentPrice);

                            char tempExecutedPriceAmountString[80];
                            char tempAmountExecutedStr[30];
                            sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                            stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                            executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));
                        
                            strcpy(listToDelete[deleteCounter],ln->ele);
                            ++deleteCounter;

                            if (price == currentPrice)
                            {
                                if (amountLeftAfterExecuteOrder > 0)
                                {
                                    AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketBid,price,  amountLeftAfterExecuteOrder,&keyFieldsds,keyFieldKey,&getsetValueField);
                                }
                                else
                                {                                   
                                    long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                    if (orginalOrdersAmountLeft > 0)
                                    {
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                        stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                        addBack = 1;
                                        addBackPrice = currentPrice;
                                    }
                                }
                                break;
                            }
                            else
                            {
                                if (amountLeftAfterExecuteOrder <= 0)
                                {
                                    long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                    if (orginalOrdersAmountLeft > 0)
                                    {
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                        stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                        addBack = 1;
                                        addBackPrice = currentPrice;
                                    }
                                    break;
                                }
                            }
                            amountRemain = amountLeftAfterExecuteOrder;
                        }
                        if (llen == 0)
                        {
                            if (amountRemain > 0 && price > currentPrice)
                            {                                /*add to bid list and key  */
                                AddOrderToSelfSide(c, orderId,orderIdLongLong,  buySell, marketBid,price, amountRemain,&keyFieldsds,keyFieldKey,&getsetValueField);                              
                            }
                        }
                    }
                    ln = ln->level[0].forward;
                }
            }
            if(totalAmountExecuted>0){
                averageExecutedPrice = executedPriceAmount / totalAmountExecuted;
            }

            keyFieldsds = sds88replace(keyFieldsds,marketAsk, marketAskLen);
            if(deleteCounter){
                int deleted = 0;
                
                for (int i = 0; i < deleteCounter; ++i)
                {
                    getsetValueField = sds88replace(getsetValueField,listToDelete[i],strlen(listToDelete[i]));
                    if (zsetDel(askrobj, getsetValueField))
                        ++deleted;
                    if (zsetLength(askrobj) == 0)
                    {
                        if(!addBack){
                            dbDelete(c->db, keyFieldKey);
                            ++server.dirty;
                            break;
                        }
                    }
                }
                if(addBack){
                     getsetValueField = sds88replace(getsetValueField,amountMarketPriceAddBack,strlen(amountMarketPriceAddBack));
                     singleZAddWithKeyObj(c,keyFieldKey,askrobj,getsetValueField,addBackPrice);                     
                }
                if (deleted)
                { 
                    signalModifiedKey(c->db, keyFieldKey);
                    server.dirty += deleted;
                }
            }
            GenerateOutputJson(c, &executedOrderStrings, &executedPriceAmountStrings, orderId, averageExecutedPrice,totalAmountExecuted,&keyFieldsds,keyFieldKey);

        }
    }

    sdsfree(executedOrderStrings);
    sdsfree(executedPriceAmountStrings);
    sdsfree(keyFieldsds);
    sdsfree(getsetValueField);
    zfree(keyFieldKey);
    
    return;
}
void PlaceSellLimitedOrderCommand(client *c)
{
     /*
    argv 1 = market
    argv 2 = userId     
    argv 3 = price decimal format 
    argv 4 = amount decimal format 
    argv 5 = price
    argv 6 = amount
    */
    char *buySell = "0";
    char orderId[30] ;
    char *tempstrtodigitPtr;
    
    long double price = strtold(c->argv[5]->ptr,&tempstrtodigitPtr); 
    long double amount = strtold(c->argv[6]->ptr,&tempstrtodigitPtr); 


    long double averageExecutedPrice = 0;
    long double executedPriceAmount = 0;
    long double totalAmountExecuted = 0;

    sds executedOrderStrings = sdsempty8k();
    sds executedPriceAmountStrings = sdsempty8k();

    char marketBid[20];
    char marketAsk[20];   
    stpcpy(stpcpy(marketBid,c->argv[1]->ptr),"BID");
    stpcpy(stpcpy(marketAsk,c->argv[1]->ptr),"ASK");

    long long orderIdLongLong = getCurrentTimeStampLongLong();  
    sprintf(orderId, "%lld", orderIdLongLong); 

    size_t marketBidLen =  strlen(marketBid);
    sds keyFieldsds = sdsnew88len(marketBid,marketBidLen);  
    robj *keyFieldKey = createObject(OBJ_STRING,keyFieldsds);
    sds getsetValueField = sdsempty88();
    robj *bidrobj;

    bidrobj = lookupKeyWrite(c->db, keyFieldKey);
    if (bidrobj == NULL)
    {
        AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketAsk,price, amount,&keyFieldsds,keyFieldKey,&getsetValueField);
        /*return data*/
        GenerateOutputJson(c, &executedOrderStrings, &executedPriceAmountStrings, orderId, averageExecutedPrice,totalAmountExecuted,&keyFieldsds,keyFieldKey);
    }
    else
    {
        if (bidrobj->type != OBJ_ZSET)
        {
            int deleted = dbSyncDelete(c->db, keyFieldKey);
            if (deleted)
            {
                signalModifiedKey(c->db, keyFieldKey);
                server.dirty++;
                AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketAsk,price, amount,&keyFieldsds,keyFieldKey,&getsetValueField);
                /*return data*/
                GenerateOutputJson(c,  &executedOrderStrings, &executedPriceAmountStrings, orderId, averageExecutedPrice,totalAmountExecuted,&keyFieldsds,keyFieldKey); 
            }
        }
        else
        {
            int llen = zsetLength(bidrobj);
            long double amountRemain = amount;

            char listToDelete[llen][80]; 
            char amountMarketPriceAddBack[80];
            char currentAmountMarketPrice[80];
            char currentAmount[30];
            char currentPriceStr[30];

            long double addBackPrice = 0; 
            int deleteCounter = 0;
            int addBack = 0;
           
            if (bidrobj->encoding == OBJ_ENCODING_ZIPLIST)
            {
                unsigned char *zl = bidrobj->ptr;
                unsigned char *eptr, *sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;

                eptr = ziplistIndex(zl, -2);

                sptr = ziplistNext(zl, eptr);

                while (llen-- && amountRemain > 0)
                {
                    long double currentOrderListAmount = 0;
                    long double currentPrice;

                    ziplistGet(eptr, &vstr, &vlen, &vlong);
                    if (vstr == NULL){
                        sprintf(currentAmountMarketPrice, "%lld", vlong);
                    }
                    else{
                        strncpy(currentAmountMarketPrice,(char *)vstr, vlen);
                        currentAmountMarketPrice[vlen]='\0';
                    }
                   
                    currentPrice = zzlGetScore(sptr);                    
                    
                    if (price > currentPrice)
                    {
                        AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell,marketAsk,price, amountRemain,&keyFieldsds,keyFieldKey,&getsetValueField);
                        break;
                    }
                    else
                    {
                        strcpy(listToDelete[deleteCounter],currentAmountMarketPrice);
                        ++deleteCounter;
                        tempstrtodigitPtr = NULL;
                        strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketBid,&tempstrtodigitPtr));
                        strcpy(currentPriceStr,strtok_r(NULL, marketBid,&tempstrtodigitPtr));
                        tempstrtodigitPtr = NULL;        
                        currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr); 
                        char currentMarketSidePrice[40];
                        stpcpy(stpcpy(currentMarketSidePrice, marketBid),currentPriceStr);
                       
                        long double amountLeftAfterExecuteOrder = ExecuteStatedPriceOrders(c, currentMarketSidePrice, amountRemain,  &executedOrderStrings,&keyFieldsds,keyFieldKey,&getsetValueField);
                        long double amountExecuted;
                        if (amountLeftAfterExecuteOrder > 0)
                        {
                            amountExecuted = currentOrderListAmount;
                        }
                        else
                        {
                            amountExecuted = amountRemain;
                        }
                        totalAmountExecuted += amountExecuted;
                        executedPriceAmount += (amountExecuted *  currentPrice);

                        char tempExecutedPriceAmountString[80];
                        char tempAmountExecutedStr[30];
                        sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                        stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                        executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));
                        
                       

                        if (price == currentPrice)
                        {
                            if (amountLeftAfterExecuteOrder > 0)
                            {
                                AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketAsk,price,amountLeftAfterExecuteOrder,&keyFieldsds,keyFieldKey,&getsetValueField);
                            }
                            else
                            {
                                long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                if (orginalOrdersAmountLeft > 0)
                                {
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                    stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                    addBackPrice = currentPrice;
                                    addBack = 1;
                                }
                            }
                            break;
                        }
                        else
                        {
                            if (amountLeftAfterExecuteOrder <= 0)
                            {
                                long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                if (orginalOrdersAmountLeft > 0)
                                {
                                    char tempAmountLeft[30];
                                    sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                    stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                    addBackPrice = currentPrice;
                                    addBack = 1;
                                }
                                break;
                            }
                        }
                        amountRemain = amountLeftAfterExecuteOrder;                  
                    }                    
                    if (llen == 0)
                    {
                        if (amountRemain > 0 && price < currentPrice)
                        {
                            AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketAsk,price, amountRemain,&keyFieldsds,keyFieldKey,&getsetValueField);
                        }
                    }

                    zzlPrev(zl, &eptr, &sptr);
                }
            }
            else if (bidrobj->encoding == OBJ_ENCODING_SKIPLIST)
            {
                zset *zs = bidrobj->ptr;
                zskiplist *zsl = zs->zsl;
                zskiplistNode *ln;

                ln = zsl->tail;

                while (llen-- && amountRemain > 0)
                {
                    if (ln != NULL)
                    {
                        strcpy(currentAmountMarketPrice,ln->ele);                        
                        long double currentOrderListAmount = 0;
                        long double currentPrice;

                        currentPrice = ln->score;
                       
                       
                        if (price > currentPrice)
                        {
                            AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketAsk,price,amountRemain,&keyFieldsds,keyFieldKey,&getsetValueField);
                            break;
                        }
                        else
                        {
                            strcpy(listToDelete[deleteCounter],ln->ele);
                            ++deleteCounter;
                            tempstrtodigitPtr = NULL;
                            strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketBid,&tempstrtodigitPtr));
                            strcpy(currentPriceStr,strtok_r(NULL, marketBid,&tempstrtodigitPtr));
                            tempstrtodigitPtr = NULL;
                            currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr); 
                            char currentMarketSidePrice[40];
                            stpcpy(stpcpy(currentMarketSidePrice, marketBid), currentPriceStr);    
                            
                            long double amountLeftAfterExecuteOrder = ExecuteStatedPriceOrders(c, currentMarketSidePrice, amountRemain, &executedOrderStrings,&keyFieldsds,keyFieldKey,&getsetValueField);
                            long double amountExecuted;
                            if (amountLeftAfterExecuteOrder > 0)
                            {
                                amountExecuted = currentOrderListAmount;
                            }
                            else
                            {
                                amountExecuted = amountRemain;
                            }
                            totalAmountExecuted += amountExecuted;
                            executedPriceAmount += (amountExecuted *  currentPrice);

                            char tempExecutedPriceAmountString[80];
                            char tempAmountExecutedStr[30];
                            sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                            stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                            executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));
                            
                            

                            if (price == currentPrice)
                            {
                                if (amountLeftAfterExecuteOrder > 0)
                                {
                                    AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketAsk,price, amountLeftAfterExecuteOrder,&keyFieldsds,keyFieldKey,&getsetValueField);
                                }
                                else
                                {
                                    long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                    if (orginalOrdersAmountLeft > 0)
                                    {
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                        stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                        addBackPrice = currentPrice;
                                        addBack = 1;                                      
                                    }
                                }  
                                break;
                            }
                            else
                            {
                                if (amountLeftAfterExecuteOrder <= 0)
                                {
                                    long double orginalOrdersAmountLeft = currentOrderListAmount - amountRemain;
                                    if (orginalOrdersAmountLeft > 0)
                                    {
                                        char tempAmountLeft[30];
                                        sprintf(tempAmountLeft,c->argv[4]->ptr,orginalOrdersAmountLeft);
                                        stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                        addBackPrice = currentPrice;
                                        addBack = 1;             
                                    }    
                                    break;
                                }
                            }
                            amountRemain = amountLeftAfterExecuteOrder;
                        }
                        if (llen == 0)
                        {
                            if (amountRemain > 0 && price < currentPrice)
                            {
                                AddOrderToSelfSide(c, orderId,orderIdLongLong, buySell, marketAsk,price,  amountRemain , &keyFieldsds,keyFieldKey,&getsetValueField);
                            }
                        }
                    }
                    ln = ln->backward;
                }
            }
            if(totalAmountExecuted>0){
                averageExecutedPrice = executedPriceAmount / totalAmountExecuted;
            }
            keyFieldsds = sds88replace(keyFieldsds,marketBid, marketBidLen);            
            if(deleteCounter){
                int deleted = 0;
                
                for (int i = 0; i < deleteCounter; ++i)
                {
                    getsetValueField = sds88replace(getsetValueField,listToDelete[i],strlen(listToDelete[i]));             
                    if (zsetDel(bidrobj, getsetValueField))
                        ++deleted;
                    if (zsetLength(bidrobj) == 0)
                    {
                        if(!addBack){
                            dbDelete(c->db, keyFieldKey);
                            ++server.dirty;
                            break;
                        }
                    }
                }
                if(addBack){
                     getsetValueField = sds88replace(getsetValueField,amountMarketPriceAddBack,strlen(amountMarketPriceAddBack));
                     singleZAddWithKeyObj(c,keyFieldKey,bidrobj,getsetValueField,addBackPrice);                   
                }
                if (deleted)
                { 
                    signalModifiedKey(c->db, keyFieldKey);
                    server.dirty += deleted;
                }
            }
           
            GenerateOutputJson(c,  &executedOrderStrings, &executedPriceAmountStrings, orderId, averageExecutedPrice,totalAmountExecuted,&keyFieldsds,keyFieldKey);
           
        }
    }

   
   
    sdsfree(executedOrderStrings);
    sdsfree(executedPriceAmountStrings);
    sdsfree(keyFieldsds);
    sdsfree(getsetValueField);
    zfree(keyFieldKey);
    return;
}
void PlaceBuyMarketOrderCommand(client *c)
{
     /*
    argv 1 = market     
    argv 2 = isContractCash 1 =true 0 = false , false also use for non contract calc
    argv 3 = contractMultiplier non contract also = 1 
    argv 4 = price decimal Format 
    argv 5 = amount decimal Int 
    argv 6 = amount decimal Format 
    argv 7 = coin decimal Int
    argv 8 = coin decimal Format
    argv 9 = amount
    argv 10 = money 
    argv 11 = margin Rate (1 for non contract)
    */

    char orderId[30];
    char *tempstrtodigitPtr;    

    char *isContract = c->argv[2]->ptr;
    double contractMultiplier = strtod(c->argv[3]->ptr,&tempstrtodigitPtr);   

    long double averageExecutedPrice = 0;
    long double executedPriceAmount = 0;
    long double totalAmountExecuted = 0;

    int amountDecimalInt = strtol(c->argv[5]->ptr,&tempstrtodigitPtr,10);
    int coinDecimalInt = strtol(c->argv[7]->ptr,&tempstrtodigitPtr,10);

    long double amount  = strtold(c->argv[9]->ptr,&tempstrtodigitPtr); 
    long double money = strtold(c->argv[10]->ptr,&tempstrtodigitPtr); 
    double marginRate = strtod(c->argv[11]->ptr,&tempstrtodigitPtr); 

    sds executedOrderStrings = sdsempty8k();
    sds executedPriceAmountStrings = sdsempty8k();
    
    if (marginRate == 0)
    {
        marginRate = 1;
    }
    
    char marketBid[20];
    char marketAsk[20];   
    stpcpy(stpcpy(marketBid,c->argv[1]->ptr),"BID");
    stpcpy(stpcpy(marketAsk,c->argv[1]->ptr),"ASK");
    long long orderIdLongLong = getCurrentTimeStampLongLong();  
    sprintf(orderId, "%lld", orderIdLongLong);

    size_t marketAskLen =  strlen(marketAsk);
    sds keyFieldsds = sdsnew88len(marketAsk,marketAskLen);
    robj *keyFieldKey = createObject(OBJ_STRING,keyFieldsds);
    sds getsetValueField = sdsempty88();
    robj *askrobj;
    
    askrobj = lookupKeyWrite(c->db, keyFieldKey);
    if (askrobj == NULL)
    {
        
        GenerateMarketOutputJson(c,orderId,money, &executedOrderStrings, &executedPriceAmountStrings, averageExecutedPrice, totalAmountExecuted,&keyFieldsds,keyFieldKey);       
    }
    else
    {
        if (askrobj->type != OBJ_ZSET)
        {
            int deleted = dbSyncDelete(c->db, keyFieldKey);
            if (deleted)
            {
                signalModifiedKey(c->db, keyFieldKey);
                server.dirty++;
                GenerateMarketOutputJson(c,orderId,money, &executedOrderStrings, &executedPriceAmountStrings, averageExecutedPrice, totalAmountExecuted,&keyFieldsds,keyFieldKey);                       
            }
        }
        else
        {
            
            int llen = zsetLength(askrobj);
            long double amountRemain = amount;
            char listToDelete[llen][80]; 
            char amountMarketPriceAddBack[80];
            char currentAmountMarketPrice[80];
            char currentAmount[30];
            char currentPriceStr[30];

            long long amountdecimalPow = pow(10, amountDecimalInt);
            long double addBackPrice = 0; 
            int deleteCounter = 0;
            int addBack = 0;
            if (askrobj->encoding == OBJ_ENCODING_ZIPLIST)
            {
                char dupAmountMarketPrice[80];
                
                
                unsigned char *zl = askrobj->ptr;
                unsigned char *eptr, *sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;

                
                eptr = ziplistIndex(zl, 0);

                sptr = ziplistNext(zl, eptr);
                while (llen-- && amountRemain > 0)
                {
                    
                    long double currentOrderListAmount;
                    long double currentPrice;

                    ziplistGet(eptr, &vstr, &vlen, &vlong);
                    if (vstr == NULL){
                        sprintf(currentAmountMarketPrice, "%lld", vlong);
                    }
                    else{
                        strncpy(currentAmountMarketPrice,(char *)vstr, vlen);
                        currentAmountMarketPrice[vlen]='\0';                       
                    }
                    strcpy(dupAmountMarketPrice,currentAmountMarketPrice);
                    currentPrice =  zzlGetScore(sptr);

                    tempstrtodigitPtr = NULL;
                    strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketAsk,&tempstrtodigitPtr));
                    strcpy(currentPriceStr,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));  
                    tempstrtodigitPtr = NULL;
                    currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr); 

                    
                    char currentMarketSidePrice[50];
                    stpcpy(stpcpy(currentMarketSidePrice, marketAsk), currentPriceStr);

                    
                    long double orderAmountLeftAfterExecuteOrder = ExecuteStatedMarketPriceOrders(c, currentMarketSidePrice, &amountRemain, &money, marginRate, &executedOrderStrings, amountdecimalPow, currentOrderListAmount, currentPrice, isContract, contractMultiplier,&keyFieldsds,keyFieldKey,&getsetValueField);
                    
                    if (orderAmountLeftAfterExecuteOrder < currentOrderListAmount)
                    {
                        
                        long double amountExecuted = currentOrderListAmount - orderAmountLeftAfterExecuteOrder;

                        totalAmountExecuted += amountExecuted;
                        executedPriceAmount += (amountExecuted *  currentPrice);

                        char tempExecutedPriceAmountString[80];
                        char tempAmountExecutedStr[30];
                        sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                        stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                        executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));

                        
                        strcpy(listToDelete[deleteCounter],dupAmountMarketPrice);
                        ++deleteCounter;

                        
                        if (orderAmountLeftAfterExecuteOrder > 0)
                        {
                            char tempAmountLeft[30];
                            sprintf(tempAmountLeft,c->argv[6]->ptr,orderAmountLeftAfterExecuteOrder);
                            stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                            addBackPrice = currentPrice;
                            addBack = 1;
                        }
                    }
                    zzlNext(zl, &eptr, &sptr);
                }
            }
            else if (askrobj->encoding == OBJ_ENCODING_SKIPLIST)
            {
                zset *zs = askrobj->ptr;
                zskiplist *zsl = zs->zsl;
                zskiplistNode *ln;

                
                ln = zsl->header->level[0].forward;

                while (llen-- && amountRemain > 0)
                {
                    if (ln != NULL)
                    {
                        strcpy(currentAmountMarketPrice,ln->ele); 
                        long double currentOrderListAmount;
                        long double currentPrice;

                        currentPrice = ln->score;

                        tempstrtodigitPtr = NULL;
                        strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketAsk,&tempstrtodigitPtr));
                        strcpy(currentPriceStr,strtok_r(NULL, marketAsk,&tempstrtodigitPtr));        
                        tempstrtodigitPtr = NULL;
                        currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr); 

                        
                        char currentMarketSidePrice[50];
                        stpcpy(stpcpy(currentMarketSidePrice, marketAsk), currentPriceStr);                        
                       
                        long double orderAmountLeftAfterExecuteOrder = ExecuteStatedMarketPriceOrders(c, currentMarketSidePrice, &amountRemain, &money, marginRate, &executedOrderStrings, amountdecimalPow, currentOrderListAmount, currentPrice, isContract, contractMultiplier,&keyFieldsds,keyFieldKey,&getsetValueField);

                        if (orderAmountLeftAfterExecuteOrder < currentOrderListAmount)
                        {
                            long double amountExecuted = currentOrderListAmount - orderAmountLeftAfterExecuteOrder;

                            totalAmountExecuted += amountExecuted;
                            executedPriceAmount += (amountExecuted *  currentPrice);

                            char tempExecutedPriceAmountString[80];
                            char tempAmountExecutedStr[30];
                            sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                            stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                            executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));

                            
                            strcpy(listToDelete[deleteCounter],ln->ele);
                            ++deleteCounter;

                            
                            if (orderAmountLeftAfterExecuteOrder > 0)
                            {
                                char tempAmountLeft[30];
                                sprintf(tempAmountLeft,c->argv[6]->ptr,orderAmountLeftAfterExecuteOrder);
                                stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                addBackPrice = currentPrice;
                                addBack = 1;
                            }
                        }
                        
                    }
                    ln = ln->level[0].forward;
                }
            }
            
            
            long long coinDecimalPow = pow(10, coinDecimalInt);   
            money = floor(money * coinDecimalPow) / coinDecimalPow;
            if(totalAmountExecuted>0){
                averageExecutedPrice = executedPriceAmount/totalAmountExecuted;
            }
            keyFieldsds = sds88replace(keyFieldsds,marketAsk, marketAskLen);            
            if(deleteCounter){
                int deleted = 0;               
                for (int i = 0; i < deleteCounter; ++i)
                {
                    getsetValueField = sds88replace(getsetValueField,listToDelete[i],strlen(listToDelete[i]));
                    
                    if (zsetDel(askrobj, getsetValueField))
                        ++deleted;
                    if (zsetLength(askrobj) == 0)
                    {
                        if(!addBack){
                            dbDelete(c->db, keyFieldKey);
                            ++server.dirty;
                            break;
                        }
                    }
                }
                if(addBack){
                    getsetValueField = sds88replace(getsetValueField,amountMarketPriceAddBack,strlen(amountMarketPriceAddBack));
                    singleZAddWithKeyObj(c,keyFieldKey,askrobj,getsetValueField,addBackPrice);  
                }
                if (deleted)
                { 
                    signalModifiedKey(c->db, keyFieldKey);
                    server.dirty += deleted;
                }
            }
            GenerateMarketOutputJson(c,orderId,money, &executedOrderStrings, &executedPriceAmountStrings, averageExecutedPrice, totalAmountExecuted,&keyFieldsds,keyFieldKey);                       
        }
    }
    
    sdsfree(executedOrderStrings);
    sdsfree(executedPriceAmountStrings);
    sdsfree(keyFieldsds);
    sdsfree(getsetValueField);
    zfree(keyFieldKey);
    
    
    return;
}
void PlaceSellMarketOrderCommand(client *c)
{
     /*
    argv 1 = market     
    argv 2 = isContractCash 1 =true 0 = false , false also use for non contract calc
    argv 3 = contractMultiplier non contract also = 1 
    argv 4 = price decimal Format 
    argv 5 = amount decimal Int 
    argv 6 = amount decimal Format 
    argv 7 = coin decimal Int
    argv 8 = coin decimal Format
    argv 9 = amount
    argv 10 = money 
    argv 11 = margin Rate (1 for non contract)
    */

    char orderId[30];
    char *tempstrtodigitPtr; 

    char *isContract = c->argv[2]->ptr;
    double contractMultiplier = strtod(c->argv[3]->ptr,&tempstrtodigitPtr);   

    long double averageExecutedPrice = 0;
    long double executedPriceAmount = 0;
    long double totalAmountExecuted = 0;

    int amountDecimalInt = strtol(c->argv[5]->ptr,&tempstrtodigitPtr,10);
    int coinDecimalInt = strtol(c->argv[7]->ptr,&tempstrtodigitPtr,10);

    long double amount  = strtold(c->argv[9]->ptr,&tempstrtodigitPtr); 
    long double money = strtold(c->argv[10]->ptr,&tempstrtodigitPtr); 
    double marginRate = strtod(c->argv[11]->ptr,&tempstrtodigitPtr); 

    sds executedOrderStrings = sdsempty8k();
    sds executedPriceAmountStrings = sdsempty8k();
   
    if (marginRate == 0)
    {
        marginRate = 1;
    }
    
    char marketBid[20];
    char marketAsk[20];   
    stpcpy(stpcpy(marketBid,c->argv[1]->ptr),"BID");
    stpcpy(stpcpy(marketAsk,c->argv[1]->ptr),"ASK");
    long long orderIdLongLong = getCurrentTimeStampLongLong();  
    sprintf(orderId, "%lld", orderIdLongLong); 

    size_t marketBidLen =  strlen(marketBid);
    sds keyFieldsds = sdsnew88len(marketBid,marketBidLen);  
    robj *keyFieldKey = createObject(OBJ_STRING,keyFieldsds);
    sds getsetValueField = sdsempty88();
    robj *bidrobj;
    
    bidrobj = lookupKeyWrite(c->db, keyFieldKey);
    if (bidrobj == NULL)
    {
        GenerateMarketOutputJson(c,orderId,money, &executedOrderStrings, &executedPriceAmountStrings, averageExecutedPrice, totalAmountExecuted,&keyFieldsds,keyFieldKey);       
    }
    else
    {
        if (bidrobj->type != OBJ_ZSET)
        {
            int deleted = dbSyncDelete(c->db, keyFieldKey);
            if (deleted)
            {
                signalModifiedKey(c->db, keyFieldKey);
                server.dirty++;
                GenerateMarketOutputJson(c,orderId,money, &executedOrderStrings, &executedPriceAmountStrings, averageExecutedPrice, totalAmountExecuted,&keyFieldsds,keyFieldKey);       
            }
        }
        else
        {
            
            int llen = zsetLength(bidrobj);
            long double amountRemain = amount;
           
            char listToDelete[llen][80]; 
            char amountMarketPriceAddBack[80];
            char currentAmountMarketPrice[80];
            char currentAmount[30];
            char currentPriceStr[30];

            long long amountdecimalPow = pow(10, amountDecimalInt);
            long double addBackPrice = 0; 
            int deleteCounter = 0;
            int addBack = 0;
            if (bidrobj->encoding == OBJ_ENCODING_ZIPLIST)
            {
                char dupAmountMarketPrice[80];
                unsigned char *zl = bidrobj->ptr;
                unsigned char *eptr, *sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;

                
                eptr = ziplistIndex(zl, -2);

                sptr = ziplistNext(zl, eptr);

                while (llen-- && amountRemain > 0)
                {                                       
                    long double currentOrderListAmount;
                    long double currentPrice;

                    ziplistGet(eptr, &vstr, &vlen, &vlong);
                    if (vstr == NULL){
                        sprintf(currentAmountMarketPrice, "%lld", vlong);
                    }
                    else{
                        strncpy(currentAmountMarketPrice,(char *)vstr, vlen);
                        currentAmountMarketPrice[vlen]='\0';
                    }
                    strcpy(dupAmountMarketPrice,currentAmountMarketPrice);
                    currentPrice =  zzlGetScore(sptr);
                    tempstrtodigitPtr = NULL;
                    strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketBid,&tempstrtodigitPtr));
			        strcpy(currentPriceStr,strtok_r(NULL, marketBid,&tempstrtodigitPtr));        
                    tempstrtodigitPtr = NULL;
                    currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr);                    

                    
                    char currentMarketSidePrice[50];
                    stpcpy(stpcpy(currentMarketSidePrice, marketBid), currentPriceStr);
                    

                    long double orderAmountLeftAfterExecuteOrder =  ExecuteStatedMarketPriceOrders(c, currentMarketSidePrice, &amountRemain, &money, marginRate, &executedOrderStrings, amountdecimalPow, currentOrderListAmount, currentPrice, isContract, contractMultiplier,&keyFieldsds,keyFieldKey,&getsetValueField);

                    if (orderAmountLeftAfterExecuteOrder < currentOrderListAmount)
                    {
                        long double amountExecuted = currentOrderListAmount - orderAmountLeftAfterExecuteOrder;

                        totalAmountExecuted += amountExecuted;
                        executedPriceAmount += (amountExecuted *  currentPrice);

                        char tempExecutedPriceAmountString[80];
                        char tempAmountExecutedStr[30];
                        sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                        stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                        executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));

                        strcpy(listToDelete[deleteCounter],dupAmountMarketPrice);
                        ++deleteCounter;

                        
                        if (orderAmountLeftAfterExecuteOrder > 0)
                        {
                            char tempAmountLeft[30];
                            sprintf(tempAmountLeft,c->argv[6]->ptr,orderAmountLeftAfterExecuteOrder);
                            stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                            addBackPrice = currentPrice;
                            addBack = 1;
                        }
                    }             

                    zzlPrev(zl, &eptr, &sptr);
                }
            }
            else if (bidrobj->encoding == OBJ_ENCODING_SKIPLIST)
            {
                zset *zs = bidrobj->ptr;
                zskiplist *zsl = zs->zsl;
                zskiplistNode *ln;

                
                ln = zsl->tail;

                while (llen-- && amountRemain > 0)
                {
                    if (ln != NULL)
                    {
                        strcpy(currentAmountMarketPrice,ln->ele);
                        long double currentOrderListAmount;
                        long double currentPrice;

                        currentPrice = ln->score;
                        tempstrtodigitPtr = NULL;
                        strcpy(currentAmount,strtok_r(currentAmountMarketPrice, marketBid,&tempstrtodigitPtr));
                        strcpy(currentPriceStr,strtok_r(NULL, marketBid,&tempstrtodigitPtr));
                        tempstrtodigitPtr = NULL;
                        currentOrderListAmount = strtold(currentAmount,&tempstrtodigitPtr); 
                       
                        
                        char currentMarketSidePrice[50];
                        stpcpy(stpcpy(currentMarketSidePrice, marketBid), currentPriceStr);                        
                        
                        long double orderAmountLeftAfterExecuteOrder = ExecuteStatedMarketPriceOrders(c, currentMarketSidePrice, &amountRemain, &money, marginRate, &executedOrderStrings, amountdecimalPow, currentOrderListAmount, currentPrice, isContract, contractMultiplier,&keyFieldsds,keyFieldKey,&getsetValueField);

                        if (orderAmountLeftAfterExecuteOrder < currentOrderListAmount)
                        {
                            long double amountExecuted = currentOrderListAmount - orderAmountLeftAfterExecuteOrder;

                            totalAmountExecuted += amountExecuted;
                            executedPriceAmount += (amountExecuted *  currentPrice);

                            char tempExecutedPriceAmountString[80];
                            char tempAmountExecutedStr[30];
                            sprintf(tempAmountExecutedStr,c->argv[4]->ptr,amountExecuted);
                            stpcpy(stpcpy(stpcpy(stpcpy(stpcpy(tempExecutedPriceAmountString,",{\"Price\":"), currentPriceStr), ",\"Amount\":"),tempAmountExecutedStr),"}");
                            executedPriceAmountStrings = sdscat8klen(executedPriceAmountStrings,tempExecutedPriceAmountString,strlen(tempExecutedPriceAmountString));
                            
                            
                            strcpy(listToDelete[deleteCounter],ln->ele);
                            ++deleteCounter;

                            
                            if (orderAmountLeftAfterExecuteOrder > 0)
                            {
                                char tempAmountLeft[30];
                                sprintf(tempAmountLeft,c->argv[6]->ptr,orderAmountLeftAfterExecuteOrder);
                                stpcpy(stpcpy(amountMarketPriceAddBack,tempAmountLeft),currentMarketSidePrice);
                                addBackPrice = currentPrice;
                                addBack = 1;
                            }
                        }              
                    }
                    ln = ln->backward;
                }
            }
            
            long long coinDecimalPow = pow(10, coinDecimalInt);   
            money = floor(money * coinDecimalPow) / coinDecimalPow;
            if(totalAmountExecuted>0){
                averageExecutedPrice = executedPriceAmount/totalAmountExecuted;
            }
            keyFieldsds = sds88replace(keyFieldsds,marketBid, marketBidLen); 
            if(deleteCounter){
                int deleted = 0;
                for (int i = 0; i < deleteCounter; ++i)
                {
                    getsetValueField = sds88replace(getsetValueField,listToDelete[i],strlen(listToDelete[i]));                    
                    
                    if (zsetDel(bidrobj, getsetValueField))
                        ++deleted;
                    if (zsetLength(bidrobj) == 0)
                    {
                        if(!addBack){
                            dbDelete(c->db, keyFieldKey);
                            ++server.dirty;
                            break;
                        }
                    }
                }
                if(addBack){
                     getsetValueField = sds88replace(getsetValueField,amountMarketPriceAddBack,strlen(amountMarketPriceAddBack));
                     singleZAddWithKeyObj(c,keyFieldKey,bidrobj,getsetValueField,addBackPrice); 
                }
                if (deleted)
                { 
                    signalModifiedKey(c->db, keyFieldKey);
                    server.dirty += deleted;
                }
            }
            GenerateMarketOutputJson(c,orderId,money, &executedOrderStrings, &executedPriceAmountStrings, averageExecutedPrice, totalAmountExecuted,&keyFieldsds,keyFieldKey);                       
           
        }
    }
    
    sdsfree(executedOrderStrings);
    sdsfree(executedPriceAmountStrings);
    sdsfree(keyFieldsds);
    sdsfree(getsetValueField);
    zfree(keyFieldKey);    
    
    return;
}
void CancelOrderCommand(client *c)
{
     /*******************************
      * argv 1 = market 
      * argv 2 = amount decimal Format 
      * argv 3 = orderId .......
      * 
      * return result 1 = "userId orderId ......." for success cancel
      * return result 2 = bid ask list
      ******************************/

    
    sds returnResult = sdsempty8k(); 
    char marketSide[20];
    char *tempstrtodigitPtr;
    char orderInFirstTenBidAskRange[2] = "0";

    char orderPrice[25];
    char userId[25];
    char tempResult[60];

    sds keyFieldsds = sdsempty88();
    sds getsetValueField = sdsempty88();
    robj *keyFieldKey = createObject(OBJ_STRING,keyFieldsds);
    char oldAmountMarketPrice[80];
    char currentAmount[30];
    char currentPriceStr[30];

    for(int i=3; i < c->argc;i++)
    {
        
        

        
        robj *orderobj = lookupKeyWrite(c->db, c->argv[i]);
       
        if (orderobj != NULL)        
        {
            
            keyFieldsds = sds88replace(keyFieldsds,"Price",5);                       
            GetDataFromHash88Replace(orderobj, keyFieldsds, &getsetValueField);
            stpcpy(orderPrice,getsetValueField);
            long double price = strtold(orderPrice,&tempstrtodigitPtr); 
            

            keyFieldsds = sds88replace(keyFieldsds,"User_Id",7);                       
            GetDataFromHash88Replace(orderobj, keyFieldsds, &getsetValueField);            
            stpcpy(userId,getsetValueField);

            keyFieldsds = sds88replace(keyFieldsds,"BuySell",7);                 
            GetDataFromHash88Replace(orderobj, keyFieldsds, &getsetValueField);
            if(getsetValueField[0]=='1'){
                stpcpy(stpcpy(marketSide,c->argv[1]->ptr),"BID");
            }
            else{
                stpcpy(stpcpy(marketSide,c->argv[1]->ptr),"ASK");
            }

            keyFieldsds = sds88replace(keyFieldsds,"AmountLeft",10); 
            GetDataFromHash88Replace(orderobj, keyFieldsds, &getsetValueField);    
            long double orderAmount  = strtold(getsetValueField,&tempstrtodigitPtr); 
           

            char bidAskPrice[45];
            stpcpy(stpcpy(bidAskPrice,marketSide),orderPrice);
           
            
            int deleted = dbSyncDelete(c->db,  c->argv[i]);
            if (deleted)
            {
                signalModifiedKey(c->db,  c->argv[i]);
                server.dirty++;
            }
            
            keyFieldsds = sds88replace(keyFieldsds,bidAskPrice, strlen(bidAskPrice));
            robj *bidAskPriceobj = lookupKeyWrite(c->db, keyFieldKey);
            DeleteSingleMember(c, bidAskPriceobj, keyFieldKey, c->argv[i]->ptr);
            
            keyFieldsds = sds88replace(keyFieldsds,marketSide, strlen(marketSide));
            robj *Sideobj;
            
            Sideobj = lookupKeyWrite(c->db, keyFieldKey);
            
            zslParseRange(robj *min, robj *max, zrangespec *spec) */
            zrangespec range = {.min = price, .max = price, .minex = 0, .maxex = 0};

            long double currentPrice;
            int bidAskFound = 0;
            if (Sideobj->encoding == OBJ_ENCODING_ZIPLIST)
            {
                unsigned char *zl = Sideobj->ptr;
                unsigned char *eptr ,*sptr;
                unsigned char *vstr;
                unsigned int vlen;
                long long vlong;

                
                as we only have one so no need to loop*/
                eptr = zzlFirstInRange(zl, &range);
                sptr = ziplistNext(zl, eptr);
                if (eptr != NULL)
                {
                    
                    ziplistGet(eptr, &vstr, &vlen, &vlong);
                    
                    
                    if (vstr != NULL)
                    {
                        strncpy(oldAmountMarketPrice,(char *)vstr, vlen);
                        oldAmountMarketPrice[vlen] = '\0';
                    }
                    else
                    {
                        sprintf(oldAmountMarketPrice, "%lld", vlong);
                    }
                    currentPrice =  zzlGetScore(sptr);
                    bidAskFound = 1;
                }
            }
            else if (Sideobj->encoding == OBJ_ENCODING_SKIPLIST)
            {
                zset *zs = Sideobj->ptr;
                zskiplist *zsl = zs->zsl;
                zskiplistNode *ln;
                ln = zslFirstInRange(zsl, &range);
                if (ln != NULL)
                {
                    strcpy(oldAmountMarketPrice,ln->ele);
                    currentPrice = ln->score;
                    bidAskFound = 1;
                }
            }
            if(bidAskFound){
                getsetValueField = sds88replace(getsetValueField,oldAmountMarketPrice,strlen(oldAmountMarketPrice));
                tempstrtodigitPtr = NULL;
                strcpy(currentAmount,strtok_r(oldAmountMarketPrice, marketSide,&tempstrtodigitPtr));
                strcpy(currentPriceStr,strtok_r(NULL, marketSide,&tempstrtodigitPtr));
                tempstrtodigitPtr = NULL;
                long double amount = strtold(currentAmount,&tempstrtodigitPtr); 

                long double totalAmountLeft = amount - orderAmount;
                
                DeleteSingleMember(c, Sideobj, keyFieldKey, getsetValueField);

                if (totalAmountLeft > 0)
                {
                    char tempAmountLeft[30];
                    char tempMember[60];
                    sprintf(tempAmountLeft,c->argv[2]->ptr,totalAmountLeft);
                    stpcpy(stpcpy(stpcpy(tempMember,tempAmountLeft),marketSide),orderPrice);
                    getsetValueField = sds88replace(getsetValueField,tempMember,strlen(tempMember));

                    robj *zobj = lookupKeyWrite(c->db, keyFieldKey);
                    singleZAddWithKeyObj(c,keyFieldKey,zobj,getsetValueField,currentPrice);
                }

                
                if(orderInFirstTenBidAskRange[0] == '0'){
                    if (IsPriceInFirstTenBidAskRange(c, price,&keyFieldsds,keyFieldKey))
                    {
                        orderInFirstTenBidAskRange[0] ='1';
                    }
                }
                stpcpy(stpcpy(stpcpy(stpcpy(tempResult," "),userId)," "),c->argv[i]->ptr);
                returnResult = sdscat8klen(returnResult, tempResult,strlen(tempResult));
            }
            
        }
    }
    sds bidAskList = sdsempty8k();
    if (orderInFirstTenBidAskRange[0] == '1')
    {
        
        char tempRobotChannel[40];
        sds publishList = sdsempty8k();
        stpcpy(stpcpy(tempRobotChannel,"robot_xyz_"),c->argv[1]->ptr);
        QueryBidAskMulti(c, 10, &bidAskList,&publishList,&keyFieldsds,keyFieldKey,&getsetValueField);
        robj *messageValueKey = createObject(OBJ_STRING,publishList);
        keyFieldsds = sds88replace(keyFieldsds,tempRobotChannel,strlen(tempRobotChannel));            
            
        pubsubPublishMessage(keyFieldKey,messageValueKey);

        sdsfree(publishList);
        zfree(messageValueKey);
    }
    else
    {
        bidAskList = sdsnew("{\"Bid\":[],\"Ask\":[]}");
    }
    
    addReplyMultiBulkLen(c, 2);
    size_t returnResultSize = sdslen(returnResult);
    if(returnResultSize==0){
        addReplyBulkCBuffer(c, returnResult, 0);
    }
    else{
        addReplyBulkCBuffer(c, returnResult+1, returnResultSize-1);
    }
    addReplyBulkCBuffer(c, bidAskList, sdslen(bidAskList));


    sdsfree(keyFieldsds);
    sdsfree(getsetValueField);

    zfree(keyFieldKey);

    sdsfree(returnResult);
    sdsfree(bidAskList);
    
}

void QueryTenBidAskCommand(client *c)
{   
    /****************************
     * argv 1 = market
     * *************************/   
    sds result = sdsempty8k();
    sds keyFieldsds =sdsempty88();
    robj *keyFieldKey = createObject(OBJ_STRING,keyFieldsds);
    QueryBidAsk(c,10,&result,&keyFieldsds,keyFieldKey);   
   
    addReplyBulkCBuffer(c, result, sdslen(result));

    sdsfree(keyFieldsds);
    sdsfree(result);
    zfree(keyFieldKey);
}

