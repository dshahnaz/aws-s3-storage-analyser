from audioop import reverse
from pickle import FALSE
from tokenize import String
import boto3
import botocore.exceptions
from argparse import ArgumentParser
import sys
from prettytable import PrettyTable
from threading import Thread, Lock
import datetime


class StorageAnalyser:
    def __init__(self,arguments):
        self._arguments = arguments
        
        self._session = boto3.session.Session()
        
        s3 = self._session.resource('s3')
        self._buckets = s3.buckets.all()
        self._client = boto3.client('s3')
       
        self._metrics = {}
        self._regions = {}
            
            
    def fetch(self):
        try:
            self.__collectBucketsData()
        
        except botocore.exceptions.NoCredentialsError as e:
            print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envs or configure aws credential file : " +    e.__str__())
        except Exception as e:
            print( "Unexpected error: " +    e.__str__())
        
        self.__printBuckets()
    
    
    def __printBuckets(self):
        prettyTable = PrettyTable()
        
        prettyTable.field_names = ["Bucket name", "Region", "Creation Date", "Number of files" , "Total size (" + self._arguments.sizeType +")", "Last modified date"]#, "Cost"]
        if self._arguments.sortBy:
            self.__sortBuckets()
        for bucket in self._buckets:
            metrics = self._metrics[bucket.name]
            
            prettyTable.add_row([bucket.name, self._regions[bucket.name], bucket.creation_date, metrics["nof"], metrics["size"], metrics["lmd"]])
            
        print(prettyTable)
        if self._arguments.export:
            self.__exportTable(prettyTable)
                   
        
    def __sortBuckets(self):
        sortFunc = None
        reverse = False
        match self._arguments.sortBy:
            case "name":
                def sortByName(bucket):
                    return bucket.name
                sortFunc = sortByName
            case "region":
                def sortByRegion(bucket):
                    return self._regions[bucket.name]
                sortFunc = sortByRegion
            case "creationDate":
                def sortByCreationDate(bucket):
                    return bucket.creation_date
                sortFunc = sortByCreationDate
            case "numberOfFiles":
                def sortByNumberOfFiles(bucket):
                    return self._metrics[bucket.name]["nof"]
                sortFunc = sortByNumberOfFiles
                reverse = True
            case "totalSize":
                def sortByTotalSize(bucket):
                    return self._metrics[bucket.name]["size"]
                sortFunc = sortByTotalSize
                reverse = True
            case "lastModifiedDate":
                def sortByLastModifiedDate(bucket):
                    return self._metrics[bucket.name]["lmd"]
                sortFunc = sortByLastModifiedDate
                reverse = True
        try:
            self._buckets = sorted(self._buckets, key=sortFunc, reverse=reverse)
        except Exception as e:
            print(e)
            
    
    def __ptable_to_csv(self, table, filename, headers=True):
        raw = table.get_string()
        data = [tuple(filter(None, map(str.strip, splitline)))
            for line in raw.splitlines()
            for splitline in [line.split('|')] if len(splitline) > 1]
        if table.title is not None:
            data = data[1:]
        if not headers:
            data = data[1:]
        with open(filename, 'w') as f:
            for d in data:
                f.write('{}\n'.format(','.join(d)))
    
    
    def __exportTable(self, prettyTable):
        match self._arguments.export:
            case "csv":
                self.__ptable_to_csv(prettyTable, 'output.csv')
                
        
    def __formatSizeViaArgument(self, sizeInBytes):
        match self._arguments.sizeType:
            case "byte":
                return sizeInBytes
            case "kB":
                return "%.3f" % (sizeInBytes*1.0/1024)
            case "MB":
                return "%.3f" % (sizeInBytes*1.0/1024/1024)
            case "GB":
                return "%.3f" % (sizeInBytes*1.0/1024/1024/1024)
            case "TB":
                return "%.3f" % (sizeInBytes*1.0/1024/1024/1024/1024)
            case "PB":
                return "%.3f" % (sizeInBytes*1.0/1024/1024/1024/1024/1024)
            
        
    def __getBucketMetrics(self, bucket, lockMetrics):
        size = 0
        totalCount = 0  
        lmd = None
        for key in bucket.objects.all():
            totalCount += 1
            size += key.size
            if lmd == None:
                lmd = key.last_modified
                
            if (key.last_modified > lmd):
                lmd = key.last_modified
                
        if lmd == None:
            lmd = datetime.datetime.min
        with lockMetrics:
            self._metrics[bucket.name] = {"nof": totalCount, "size": self.__formatSizeViaArgument(size), "lmd": lmd}
       
    
    def __getS3BucketRegion(self, bucketName, lockRegions):
        response = self._client.get_bucket_location(Bucket=bucketName)
        awsRegion = response['LocationConstraint']
        if awsRegion == None:
                awsRegion = self._client.meta.region_name
        with lockRegions:
            self._regions[bucketName] = awsRegion
        
        
    def __collectBucketsData(self):
        threads = []
        
        lockRegions = Lock()
        lockMetrics = Lock()

        for bucket in self._buckets:
            tr = Thread(target=self.__getS3BucketRegion, args=(bucket.name, lockRegions))
            threads.append(tr)
            tr.start()
    
            tm = Thread(target=self.__getBucketMetrics, args=(bucket, lockMetrics))
            threads.append(tm)
            tm.start()
            
        for t in threads:
            t.join()
                    

def main():
    arguments = None
    parser = ArgumentParser(description='Fetch all s3 buckets')
    parser.add_argument("-size-type", dest="sizeType", required=False, action="store", default="GB", type=str, choices=['byte', 'kB', 'MB', 'GB', 'TB', 'PB'], help="Display size types")
    parser.add_argument("-sort-by", dest="sortBy", required=False, action="store", default="region", type=str, choices=['name', 'region', 'creationDate', 'numberOfFiles', 'totalSize', 'lastModifiedDate'], help="Sort by specified column")
    parser.add_argument("-export", dest="export", required=False, action="store", type=str, choices=['csv'], help="Export data in listed formats.Currently only csv")
    
    arguments = parser.parse_args()
    
    analiser = StorageAnalyser(arguments)
    analiser.fetch()
    
    
if __name__ == "__main__":
    main()
