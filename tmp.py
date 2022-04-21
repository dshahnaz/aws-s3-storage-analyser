import boto3
import botocore.exceptions
from argparse import ArgumentParser
import sys
from prettytable import PrettyTable
x = PrettyTable()
grep_list = None

arguments = None

def get_bucket_location_of_s3(bucket_name):
    session = boto3.session.Session()
    s3_client = session.client('s3')
    try:
        result = s3_client.get_bucket_location(Bucket=bucket_name,)
    except botocore.exceptions.ClientError as e:
        raise Exception( "boto3 client error in get_bucket_location_of_s3: " + e.__str__())
    except Exception as e:
        raise Exception( "Unexpected error in get_bucket_location_of_s3 function: " +    e.__str__())
    return result

def getS3BucketRegion(bucketName):
        client = boto3.client('s3')
        response = client.get_bucket_location(Bucket=bucketName)
        awsRegion = response['LocationConstraint']
        return awsRegion 

def main():
    try:
        session = boto3.Session()
        s3 = session.resource('s3')
        backets = s3.buckets.all()
        print("asdas")
        x.field_names = ["Bucket name", "Region", "Creation Date"]#, "Number of files", "Total size", "Last modified date", "Cost"]
        for bucket in s3.buckets.all():
            x.add_row([bucket.name, getS3BucketRegion(bucket.name), bucket.creation_date])
            size = 0
            totalCount = 0  
            for key in bucket.objects.all():
                totalCount += 1
                size += key.size
                

            # print (bucket.objects.all())
            print('total size:')
            print("%.3f GB" % (size*1.0/1024/1024/1024))
            print('total count:')
            print(totalCount)

            # print (bucket.creation_date)
            # for my_bucket_object in bucket.objects.all():
            #     print(my_bucket_object.key)
        print(x)
    except botocore.exceptions.NoCredentialsError as e:
        print(e)
        print("please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY envs or configure aws credential file")
    
    
  
        
    # print("Hello python")
    # global arguments
    # global grep_list
    # parser = ArgumentParser(description='Process some integers.')
    # parser.add_argument("-D", dest="download", required=False, action="store_true", default=False, help="Download files. This requires significant disk space.")
    # parser.add_argument("-d", dest="savedir", required=False, default='', help="If -D, then -d 1 to create save directories for each bucket with results.")
    # parser.add_argument("-l", dest="hostlist", required=True, help="") 
    # parser.add_argument("-g", dest="grepwords", required=False, help="Provide a wordlist to grep for.")
    # parser.add_argument("-m", dest="maxsize", type=int, required=False, default=1024, help="Maximum file size to download.")
    # parser.add_argument("-t", dest="threads", type=int, required=False, default=1, help="Number of threads.")

    # if len(sys.argv) == 1:
    #     # print_banner()
    #     parser.error("No arguments given.")
    #     parser.print_usage()
    #     sys.exit()

    # # output parsed arguments into a usable object
    # arguments = parser.parse_args()
    
    
if __name__ == "__main__":
    main()
