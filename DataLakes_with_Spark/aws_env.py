import configparser
import boto3
from botocore.exceptions import ClientError
import json
import time
import argparse

class AWSEnv():
    """
    Class representing AWS Env.
    It allows to create IAM role, and EMR Spark cluster.
    Cluster parameters are readed from dl.cfg file.
    It is simple implementation for this project
    """
    def __init__(self):
        """
        Class init. Configuration is readed from dwh.cfg file.
        """
        config = configparser.ConfigParser()
        config.read('dl.cfg')
        
        self.__aws_key                = config.get('AWS','AWS_ACCESS_KEY_ID')
        self.__secret                 = config.get('AWS','AWS_SECRET_ACCESS_KEY')
        self.__region                 = config.get('AWS','AWS_REGION')

        self.__emr_cluster_name       = config.get("EMR","CLUSTER_NAME")
        self.__emr_instance_type      = config.get("EMR","INSTANCE_TYPE")
        self.__emr_instance_count     = int(config.get("EMR","INSTANCE_COUNT"))
        self.__emr_sw_release         = config.get("EMR","SW_RELEASE")
        self.__emr_log_uri            = config.get("EMR","LOG_URI")
        self.__emr_script_bucket      = config.get("EMR","SCRIPT_URI")
        self.__emr_ec2_key_name       = config.get("EMR","EC2_KEY_NAME")
        
        self.__emr_iam_role_name      = config.get("EMR", "EMR_IAM_ROLE_NAME")        
        
        self.cluster_id = None
        self.roleArn    = None
      
        # EMR client, need for create EMR Spark Cluster
        self.emr = boto3.client('emr',
                                aws_access_key_id= self.__aws_key,
                                aws_secret_access_key= self.__secret,
                                region_name= self.__region
                               )
        
        # S3 client, need for upload script to AWS env
        self.s3_client = boto3.client('s3',
                                      region_name= self.__region,
                                      aws_access_key_id= self.__aws_key,
                                      aws_secret_access_key= self.__secret
                                     )
        # IAM client, to create Role for EMR to call AWS services on your behalf
        self.iam = boto3.client('iam',aws_access_key_id= self.__aws_key,
                             aws_secret_access_key= self.__secret,
                             region_name= self.__region
                          )        
                
    def __str__(self):
        return str((
            self.__aws_key,
            self.__region,            
            self.__emr_cluster_name,
            self.__emr_instance_type,
            self.__emr_instance_count,
            self.__emr_sw_release,
            self.__emr_log_uri
            ))
    
    def create_IAM_role(self):
        """
        Method creates IAM role with AmazonS3ReadOnlyAccess.
        It returns self.roleArn.
        """          

        try:
            print("[IAM] Creating a new IAM Role") 
            dwhRole = self.iam.create_role(
                Path='/',
                RoleName=self.__emr_iam_role_name,
                Description = "Allows EMR to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'elasticmapreduce.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )

            # print("[IAM] Attaching Policy")
            self.iam.attach_role_policy(RoleName=self.__emr_iam_role_name,
                                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                       )
            
            self.iam.attach_role_policy(RoleName=self.__emr_iam_role_name,
                                        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
                                       )            
        except Exception as e:
            print(e)


        self.roleArn = self.iam.get_role(RoleName=self.__emr_iam_role_name)['Role']['Arn']
        return(self.roleArn)
    
    
    def delete_IAM_role(self):
        """
        Deletes IAM Role.
        """
        if not self.iam:
            self.__create_IAM_client()
            
        #Delete the role, 
        try:
            self.iam.detach_role_policy(RoleName=self.__emr_iam_role_name,
                                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                       )
            self.iam.detach_role_policy(RoleName=self.__emr_iam_role_name,
                                        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole'
                                       )            
            self.iam.delete_role(RoleName=self.__emr_iam_role_name)
        except Exception as e:
            print(e)
            
    
    def upload_code(self, file_name):
        """
        Upload script to S3 bucket.
        """
        self.s3_client.upload_file(file_name, self.__emr_script_bucket, 'etl.py')
    
    
    def create_EMR_cluster(self):
        """
        Creates EMR cluster base on  
        https://docs.aws.amazon.com/code-samples/latest/catalog/python-emr-emr_basics.py.html
        """

        try:
            response = self.emr.run_job_flow(
                Name=self.__emr_cluster_name,
                LogUri=self.__emr_log_uri,
                ReleaseLabel=self.__emr_sw_release,
                Instances={
                    'MasterInstanceType': self.__emr_instance_type,
                    'SlaveInstanceType': self.__emr_instance_type,
                    'InstanceCount': self.__emr_instance_count,
                    'Ec2KeyName': self.__emr_ec2_key_name,
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False,
                },
                Applications=[
                    {
                        'Name': 'Spark'
                    },
                ],
                Steps=[
                    {
                        'Name': 'Setup Debugging',
                        'ActionOnFailure': 'TERMINATE_CLUSTER',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['state-pusher-script']
                        }
                    },
                    {
                        'Name': 'Setup - copy files',
                        'ActionOnFailure': 'CANCEL_AND_WAIT',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ['aws', 's3', 'cp', 's3://' + self.__emr_script_bucket, '/tmp/',
                                     '--recursive']
                        }
                    },
                    {
                        'Name': 'Run PySpark ETL',
                        'ActionOnFailure': 'CANCEL_AND_WAIT',
                        'HadoopJarStep': {
                            'Jar': 'command-runner.jar',
                            'Args': ["spark-submit", "/tmp/etl.py", "--emr"]
                        }
                    }
                ],                                
                VisibleToAllUsers=True,
                JobFlowRole='EMR_EC2_DefaultRole',
                # ServiceRole='EMR_DefaultRole',
                ServiceRole=self.__emr_iam_role_name,     
            )
            self.cluster_id = response['JobFlowId']
        except ClientError:
            print("Couldn't create cluster.")
            raise
        else:
            return self.cluster_id
           
    
    def describe_cluster(self):
        """
        Gets detailed information about a cluster.
        
        :return: The retrieved cluster information.
        """
        try:
            response = self.emr.describe_cluster(ClusterId=self.cluster_id)
            cluster = response['Cluster']
            print(f"Got data for cluster {cluster['Name']}.", )
        except ClientError:
            print(f"Couldn't get data for cluster {cluster_id}.")
            raise
        else:
            return cluster

    def terminate_cluster(self, cid = None):
        """
        Terminates a cluster. This terminates all instances in the cluster and cannot
        be undone. Any data not saved elsewhere, such as in an Amazon S3 bucket, is lost.
        """
        if not self.cluster_id:
            self.cluster_id = cid
            
        try:
            self.emr.terminate_job_flows(JobFlowIds=[self.cluster_id])
            print(f"Terminated cluster {self.cluster_id}.")
        except ClientError:
            print(f"Couldn't terminate cluster {self.cluster_id}.")
            raise

            
def create_env():
    """
    Create AWS EMR Env and run ETL script.
    Waits until EMR cluster terminates.
    """
    
    awse = AWSEnv()
    print(awse)
    print("Upload etl.py to S3")
    awse.upload_code('etl.py')
    print("Create IAM role for EMR cluster")
    awse.create_IAM_role()
    print("Create IAM role for EMR cluster")
    awse.create_EMR_cluster()
    print('Waiting for EMR ETL process end ...')
    while True:
        cluster_status = awse.describe_cluster()
        print('EMR Cluster status ' + cluster_status['Status']['State'])
        if (cluster_status['Status']['State'] == 'WAITING'):
            break
        time.sleep(15)
    print(f'REMEMBER TO TERMINATE {awse.cluster_id} EMR CLUSTER!!!.')
    # awse.terminate_cluster()
    # awse.delete_IAM_role()
    

def clean_env(cid):
    awse = AWSEnv()
    awse.terminate_cluster(cid)
    awse.delete_IAM_role()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple AWS EMR Environment setup')
    parser.add_argument("-c", "--create", help='create AWS EMR Environment and run ETL steps',
                        action="store_true")
    parser.add_argument("-t", "--terminate", help='terminate AWS EMR Cluster',
                        action="store", dest="cid")
    
    args = parser.parse_args()
    
    if args.create:
        print("CALL: create_env()")
        create_env()
    if args.cid:
        print(f"Terminating {args.cid}")
        clean_env(args.cid)
