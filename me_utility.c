#include "server.h"
#include <sys/time.h>
#include <math.h>
#include <string.h>

int hashTypeGetFromZiplist(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll);
sds hashTypeGetFromHashTable(robj *o, sds field);



sds getCurrentTimeStamp()
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    long long timestamp = (tp.tv_sec * 1000000) + tp.tv_usec;
    sds timeStampStr = sdscatprintf(sdsempty(), "%lld", timestamp);
    return timeStampStr;
}
long long getCurrentTimeStampLongLong()
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    return (tp.tv_sec * 1000000) + tp.tv_usec;
}
void getCurrentTimeStampString(char *timeStampStr)
{
    struct timeval tp;
    gettimeofday(&tp, NULL);
    long long timestamp = (tp.tv_sec * 1000000) + tp.tv_usec;
    sprintf(timeStampStr, "%lld", timestamp);
    return;
}

void singleSetAddWithKeyObj(client *c,robj *theKey, robj *setobj ,const sds member)
{    /*
    setobj = lookupKeyWrite(c->db, theKey);*/
    if (setobj == NULL)
    {
        setobj = setTypeCreate(member);
        dbAdd(c->db, theKey, setobj);
    }
    else
    {
        if (setobj->type != OBJ_SET)
        {
            int deleted = dbSyncDelete(c->db, theKey);
            if (deleted)
            {
                signalModifiedKey(c->db, theKey);
                server.dirty++;
            }
            setobj = setTypeCreate(member);
            dbAdd(c->db, theKey, setobj);
        }            
    }
    setTypeAdd(setobj, member);
    signalModifiedKey(c->db, theKey);
    server.dirty++;    

}
void singleZAddWithKeyObj(client *c,robj *theKey,robj *zobj, const sds member, long double score)
{
    static char *nanerr = "resulting score is not a number (NaN)";
    int added = 0;   /* Number of new elements added. */
    int updated = 0; /* Number of elements with updated score. */
   
    if (zobj == NULL)
    {
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(member))
        {
            zobj = createZsetObject();
        }
        else
        {
            zobj = createZsetZiplistObject();
        }
        dbAdd(c->db, theKey, zobj);
    }
    else
    {
        if (zobj->type != OBJ_ZSET)
        {
            /*delete the key if it is not zset*/
            int deleted = dbSyncDelete(c->db, theKey);
            if (deleted)
            {
                signalModifiedKey(c->db, theKey);
                server.dirty++;
            }
            /*recreate it*/
            if (server.zset_max_ziplist_entries == 0 ||
                server.zset_max_ziplist_value < sdslen(member))
            {
                zobj = createZsetObject();
            }
            else
            {
                zobj = createZsetZiplistObject();
            }
            dbAdd(c->db, theKey, zobj);
        }
    }
    double newscore;
    /* mentioned at notes 0 = none*/
    int retflags = 0;
    int retval = zsetAdd(zobj, score, member, &retflags, &newscore);
    if (retval == 0)
    {
        addReplyError(c, nanerr);
        goto cleanup;
    }
    if (retflags & ZADD_ADDED)
        added++;
    if (retflags & ZADD_UPDATED)
        updated++;
    server.dirty += (added + updated);

    cleanup:

        if (added || updated)
        {
            signalModifiedKey(c->db, theKey);
        }
}

void GetDataFromHash(robj *o, sds field, sds *returnResult)
{

    if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;
        /*-1 if not found*/
        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) != -1)
        {

            if (vstr)
            {
                *returnResult = sdscatlen(*returnResult,vstr, vlen);
            }
            else
            {   
                char tempValue[2000];
                sprintf(tempValue, "%lld", vll);
                *returnResult = sdscatlen(*returnResult ,tempValue,strlen(tempValue));
            }
        }
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        sds valueGet = hashTypeGetFromHashTable(o, field);
        if(valueGet!=NULL){
            *returnResult = sdscat(*returnResult,valueGet);
        }        
    }
}
void GetDataFromHash88Replace(robj *o, sds field, sds *returnResult)
{
    /*for short text*/
    if (o->encoding == OBJ_ENCODING_ZIPLIST)
    {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;
        /*-1 if not found*/
        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) != -1)
        {

            if (vstr)
            {
                *returnResult = sds88replace(*returnResult,vstr, vlen);
            }
            else
            {  
                char tempValue[200];
                sprintf(tempValue, "%lld", vll);
                *returnResult = sds88replace(*returnResult ,tempValue,strlen(tempValue));
            }
        }
        else{
            *returnResult[0]='\0';
        }
    }
    else if (o->encoding == OBJ_ENCODING_HT)
    {
        sds valueGet = hashTypeGetFromHashTable(o, field);
        if(valueGet!=NULL){
            *returnResult = sds88replace(*returnResult,valueGet,sdslen(valueGet));
        }      
        else{
            *returnResult[0]='\0';
        }  
    }
}



void DeleteSingleMember(client *c, robj *zobj, robj *theKey, sds memberName)
{
    int deleted = 0;    
    /*delete the old member */
    if (zsetDel(zobj, memberName))
        deleted++;
    if (zsetLength(zobj) == 0)
    {
        dbDelete(c->db, theKey);
    }
    if (deleted)
    {   
        server.dirty++;
        signalModifiedKey(c->db, theKey);
    }
}

