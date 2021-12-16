import pandas as pd
import configparser
import boto3
from botocore.exceptions import ClientError
import json
import time
import argparse

class AWSEnv():
    """
    Class representing AWS Env.
    It allows to create IAM role, and Redshift cluster.
    Cluster parameters are readed from dwh.cfg file.
    It is simple implementation for this project
    """
    def __init__(self):
        """
        Class init. Configuration is readed from dwh.cfg file.
        """
        config = configparser.ConfigParser()
        config.read('dwh.cfg')
        
        self.__aws_key                = config.get('AWS','KEY')
        self.__secret                 = config.get('AWS','SECRET')

        self.__dwh_cluster_type       = config.get("DWH","DWH_CLUSTER_TYPE")
        self.__dwh_num_nodes          = config.get("DWH","DWH_NUM_NODES")
        self.__dwh_node_type          = config.get("DWH","DWH_NODE_TYPE")

        self.__dwh_cluster_identifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        self.__dwh_db                 = config.get("DWH","DWH_DB")
        self.__dwh_db_user            = config.get("DWH","DWH_DB_USER")
        self.__dwh_db_password        = config.get("DWH","DWH_DB_PASSWORD")
        self.__dwh_port               = config.get("DWH","DWH_PORT")

        self.__dwh_iam_role_name      = config.get("DWH", "DWH_IAM_ROLE_NAME")
        
        # IAM ARN
        self.roleArn = None
        
        # AWS API Client objects
        self.ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=self.__aws_key,
                       aws_secret_access_key=self.__secret
                    )
        
        self.iam = boto3.client('iam',aws_access_key_id=self.__aws_key,
                             aws_secret_access_key=self.__secret,
                             region_name='us-west-2'
                          )
        
        self.redshift = boto3.client('redshift',
                                     region_name="us-west-2",
                                     aws_access_key_id=self.__aws_key,
                                     aws_secret_access_key=self.__secret
                                    )
             
        # AWS Redshift ClusterProps
        self.redshiftClusterProps = None
        
    def __str__(self):
        return str((
            self.__aws_key,
            self.__dwh_cluster_type,
            self.__dwh_num_nodes,
            self.__dwh_node_type
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
                RoleName=self.__dwh_iam_role_name,
                Description = "Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                       'Effect': 'Allow',
                       'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
            )

            print("[IAM] Attaching Policy")
            self.iam.attach_role_policy(RoleName=self.__dwh_iam_role_name,
                                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                                       )['ResponseMetadata']['HTTPStatusCode']
        except Exception as e:
            print(e)

        print("[IAM] Get the IAM role ARN")
        self.roleArn = self.iam.get_role(RoleName=self.__dwh_iam_role_name)['Role']['Arn']

        print(self.roleArn)
        return(self.roleArn)
    
    def create_Redshift_cluster(self):
        """
        Creates Redshift cluster
        """
            
        try:
            response = self.redshift.create_cluster(        
                #HW
                ClusterType=self.__dwh_cluster_type,
                NodeType=self.__dwh_node_type,
                NumberOfNodes=int(self.__dwh_num_nodes),

                #Identifiers & Credentials
                DBName=self.__dwh_db,
                ClusterIdentifier=self.__dwh_cluster_identifier,
                MasterUsername=self.__dwh_db_user,
                MasterUserPassword=self.__dwh_db_password,

                #Roles (for s3 access)
                IamRoles=[self.roleArn]  
            )
        except Exception as e:
            print(e)
            
        # wait for status Redshift Cluster "Available"
        __c_available = False
        while not __c_available:
            # check status
            self.redshiftClusterProps = self.redshift.describe_clusters(ClusterIdentifier=self.__dwh_cluster_identifier)['Clusters'][0]
            
            print(f"Status of {self.redshiftClusterProps['ClusterIdentifier']} is {self.redshiftClusterProps['ClusterStatus']}")
            if self.redshiftClusterProps['ClusterStatus'] == 'available':
                __c_available = True
                break
            time.sleep(5)
            
        return __c_available
    
    
    def create_Access_rules(self):
        """
        Open an incoming  TCP port to access the cluster ednpoint
        """
        try:
            vpc = self.ec2.Vpc(id=self.redshiftClusterProps['VpcId'])
            defaultSg = list(vpc.security_groups.all())[0]
            print(defaultSg)
            defaultSg.authorize_ingress(
                GroupName=defaultSg.group_name,
                CidrIp='0.0.0.0/0',
                IpProtocol='TCP',
                FromPort=int(self.__dwh_port),
                ToPort=int(self.__dwh_port)
            )
        except Exception as e:
            print(e)
    
    
    def delete_IAM_role(self):
        """
        Deletes IAM Role.
        """
        if not self.iam:
            self.__create_IAM_client()
            
        #Delete the role, 
        try:
            self.iam.detach_role_policy(RoleName=self.__dwh_iam_role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
            self.iam.delete_role(RoleName=self.__dwh_iam_role_name)
        except Exception as e:
            print(e)
            
            
    def delete_Redshift_cluster(self):
        """
        Deletes Redshift cluster and whit for finish.
        At the end it rise Exception (it's acceptable)
        """
        if not self.redshift:
            self.__create_Redshift_client()
        try:
            self.redshift.delete_cluster( ClusterIdentifier=self.__dwh_cluster_identifier,  SkipFinalClusterSnapshot=True)
        
            # wait for destroy Redshift Cluster
            while True:
                # check status
                # if Redshift Clauster will be deleted it should rise Exeption
                myClusterProps = self.redshift.describe_clusters(ClusterIdentifier=self.__dwh_cluster_identifier)['Clusters'][0]
                print(f"Status of {myClusterProps['ClusterIdentifier']} is {myClusterProps['ClusterStatus']}")
                time.sleep(5)
                
        except Exception as e:
            print(e)
            
    def prettyRedshiftProps(self):
        """
        Pretty print of Redshift Properties if Cluster exists.
        Return: pandas DataFrame with Key, Values
        """
        if not self.redshift:
            return pd.DataFrame(columns=["Key", "Value"])
        
        keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
        x = [(k, v) for k,v in self.redshiftClusterProps.items() if k in keysToShow]
        return pd.DataFrame(data=x, columns=["Key", "Value"])
    

def create_env():
    awse = AWSEnv()
    roleArn = awse.create_IAM_role()

    # create RedshiftCluster
    awse.create_Redshift_cluster()

    # (optionaly) allow access from 0.0.0.0/0 if not set
    # awse.create_Access_rules()

    # PrintRedshift Props
    awse.prettyRedshiftProps()
    print('AWS Redshift Cluster created')


def clean_env():
    awse = AWSEnv()
    awse.delete_Redshift_cluster()
    awse.delete_IAM_role()
    print('AWS Redshift Cluster deleted')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple AWS Redshift Environment setup')
    parser.add_argument("-c", "--create", help='create AWS Redshift Environment',
                        action="store_true")
    parser.add_argument("-d", "--delete", help='delete AWS Redshift Environment',
                        action="store_true")
    
    args = parser.parse_args()
    if args.create:
        create_env()
    if args.delete:
        clean_env()