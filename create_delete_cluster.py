{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 3,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "import pandas as pd\n",
    "import boto3\n",
    "import json"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Param</th>\n",
       "      <th>Value</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>DWH_CLUSTER_TYPE</td>\n",
       "      <td>multi-node</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>DWH_NUM_NODES</td>\n",
       "      <td>4</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>DWH_NODE_TYPE</td>\n",
       "      <td>dc2.large</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>DWH_CLUSTER_IDENTIFIER</td>\n",
       "      <td>dwhCluster</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>DWH_DB</td>\n",
       "      <td>dwh</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>DWH_DB_USER</td>\n",
       "      <td>dwhuser</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>DWH_DB_PASSWORD</td>\n",
       "      <td>Passw0rd</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>DWH_PORT</td>\n",
       "      <td>5439</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>8</th>\n",
       "      <td>DWH_IAM_ROLE_NAME</td>\n",
       "      <td>dwhRole</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                    Param       Value\n",
       "0        DWH_CLUSTER_TYPE  multi-node\n",
       "1           DWH_NUM_NODES           4\n",
       "2           DWH_NODE_TYPE   dc2.large\n",
       "3  DWH_CLUSTER_IDENTIFIER  dwhCluster\n",
       "4                  DWH_DB         dwh\n",
       "5             DWH_DB_USER     dwhuser\n",
       "6         DWH_DB_PASSWORD    Passw0rd\n",
       "7                DWH_PORT        5439\n",
       "8       DWH_IAM_ROLE_NAME     dwhRole"
      ]
     },
     "execution_count": 4,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "import configparser\n",
    "config = configparser.ConfigParser()\n",
    "config.read_file(open('dwh2.cfg'))\n",
    "\n",
    "KEY                    = config.get('AWS','KEY')\n",
    "SECRET                 = config.get('AWS','SECRET')\n",
    "\n",
    "DWH_CLUSTER_TYPE       = config.get(\"DWH\",\"DWH_CLUSTER_TYPE\")\n",
    "DWH_NUM_NODES          = config.get(\"DWH\",\"DWH_NUM_NODES\")\n",
    "DWH_NODE_TYPE          = config.get(\"DWH\",\"DWH_NODE_TYPE\")\n",
    "\n",
    "DWH_CLUSTER_IDENTIFIER = config.get(\"DWH\",\"DWH_CLUSTER_IDENTIFIER\")\n",
    "DWH_DB                 = config.get(\"DWH\",\"DWH_DB\")\n",
    "DWH_DB_USER            = config.get(\"DWH\",\"DWH_DB_USER\")\n",
    "DWH_DB_PASSWORD        = config.get(\"DWH\",\"DWH_DB_PASSWORD\")\n",
    "DWH_PORT               = config.get(\"DWH\",\"DWH_PORT\")\n",
    "\n",
    "DWH_IAM_ROLE_NAME      = config.get(\"DWH\", \"DWH_IAM_ROLE_NAME\")\n",
    "\n",
    "(DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)\n",
    "\n",
    "pd.DataFrame({\"Param\":\n",
    "                  [\"DWH_CLUSTER_TYPE\", \"DWH_NUM_NODES\", \"DWH_NODE_TYPE\", \"DWH_CLUSTER_IDENTIFIER\", \"DWH_DB\", \"DWH_DB_USER\", \"DWH_DB_PASSWORD\", \"DWH_PORT\", \"DWH_IAM_ROLE_NAME\"],\n",
    "              \"Value\":\n",
    "                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]\n",
    "             })"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "ec2 = boto3.resource('ec2',\n",
    "                       region_name=\"us-west-2\",\n",
    "                       aws_access_key_id=KEY,\n",
    "                       aws_secret_access_key=SECRET\n",
    "                    )\n",
    "\n",
    "s3 = boto3.resource('s3',\n",
    "                       region_name=\"us-west-2\",\n",
    "                       aws_access_key_id=KEY,\n",
    "                       aws_secret_access_key=SECRET\n",
    "                   )\n",
    "\n",
    "iam = boto3.client('iam',aws_access_key_id=KEY,\n",
    "                     aws_secret_access_key=SECRET,\n",
    "                     region_name='us-west-2'\n",
    "                  )\n",
    "\n",
    "redshift = boto3.client('redshift',\n",
    "                       region_name=\"us-west-2\",\n",
    "                       aws_access_key_id=KEY,\n",
    "                       aws_secret_access_key=SECRET\n",
    "                       )"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/customer0002_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/dwdate.tbl.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0000_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0001_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0002_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0003_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0004_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0005_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0006_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/lineorder0007_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/part0000_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/part0001_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/part0002_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/part0003_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/supplier.tbl_0000_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/supplier0001_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/supplier0002_part_00.gz')\n",
      "s3.ObjectSummary(bucket_name='awssampledbuswest2', key='ssbgz/supplier0003_part_00.gz')\n"
     ]
    }
   ],
   "source": [
    "sampleDbBucket =  s3.Bucket(\"awssampledbuswest2\")\n",
    "\n",
    "# TODO: Iterate over bucket objects starting with \"ssbgz\" and print\n",
    "for obj in sampleDbBucket.objects.filter(Prefix=\"ssbgz\"):\n",
    "    print(obj)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "1.1 Creating a new IAM Role\n",
      "An error occurred (EntityAlreadyExists) when calling the CreateRole operation: Role with name dwhRole already exists.\n"
     ]
    }
   ],
   "source": [
    "try:\n",
    "    print('1.1 Creating a new IAM Role')\n",
    "    dwhRole = iam.create_role(\n",
    "        Path='/',\n",
    "        RoleName=DWH_IAM_ROLE_NAME,\n",
    "        Description = \"Allows Redshift clusters to call AWS services on your behalf.\",\n",
    "        AssumeRolePolicyDocument=json.dumps(\n",
    "            {'Statement': [{'Action': 'sts:AssumeRole',\n",
    "               'Effect': 'Allow',\n",
    "               'Principal': {'Service': 'redshift.amazonaws.com'}}],\n",
    "             'Version': '2012-10-17'})\n",
    "    )\n",
    "\n",
    "except Exception as e:\n",
    "    print(e)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "1.3 Get the IAM role ARN\n",
      "1.3 Get the IAM role ARN\n",
      "arn:aws:iam::426847813292:role/dwhRole\n"
     ]
    }
   ],
   "source": [
    "# TODO: Get and print the IAM role ARN\n",
    "print('1.3 Get the IAM role ARN')\n",
    "iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,\n",
    "                       PolicyArn=\"arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess\"\n",
    "                      )['ResponseMetadata']['HTTPStatusCode']\n",
    "\n",
    "print(\"1.3 Get the IAM role ARN\")\n",
    "roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']\n",
    "\n",
    "print(roleArn)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "try:\n",
    "    response = redshift.create_cluster(        \n",
    "        # TODO: add parameters for hardware\n",
    "        ClusterType=DWH_CLUSTER_TYPE,\n",
    "        NodeType=DWH_NODE_TYPE,\n",
    "        NumberOfNodes=int(DWH_NUM_NODES),\n",
    "\n",
    "        # TODO: add parameters for identifiers & credentials\n",
    "        DBName=DWH_DB,\n",
    "        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,\n",
    "        MasterUsername=DWH_DB_USER,\n",
    "        MasterUserPassword=DWH_DB_PASSWORD,\n",
    "        \n",
    "        # TODO: add parameter for role (to allow s3 access)\n",
    "         IamRoles=[roleArn]\n",
    "    )\n",
    "except Exception as e:\n",
    "    print(e)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<div>\n",
       "<style scoped>\n",
       "    .dataframe tbody tr th:only-of-type {\n",
       "        vertical-align: middle;\n",
       "    }\n",
       "\n",
       "    .dataframe tbody tr th {\n",
       "        vertical-align: top;\n",
       "    }\n",
       "\n",
       "    .dataframe thead th {\n",
       "        text-align: right;\n",
       "    }\n",
       "</style>\n",
       "<table border=\"1\" class=\"dataframe\">\n",
       "  <thead>\n",
       "    <tr style=\"text-align: right;\">\n",
       "      <th></th>\n",
       "      <th>Key</th>\n",
       "      <th>Value</th>\n",
       "    </tr>\n",
       "  </thead>\n",
       "  <tbody>\n",
       "    <tr>\n",
       "      <th>0</th>\n",
       "      <td>ClusterIdentifier</td>\n",
       "      <td>dwhcluster</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>1</th>\n",
       "      <td>NodeType</td>\n",
       "      <td>dc2.large</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>2</th>\n",
       "      <td>ClusterStatus</td>\n",
       "      <td>available</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>3</th>\n",
       "      <td>MasterUsername</td>\n",
       "      <td>dwhuser</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>4</th>\n",
       "      <td>DBName</td>\n",
       "      <td>dwh</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>5</th>\n",
       "      <td>Endpoint</td>\n",
       "      <td>{'Address': 'dwhcluster.caerpwnaepms.us-west-2.redshift.amazonaws.com', 'Port': 5439}</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>6</th>\n",
       "      <td>VpcId</td>\n",
       "      <td>vpc-0123dc2be9dea8294</td>\n",
       "    </tr>\n",
       "    <tr>\n",
       "      <th>7</th>\n",
       "      <td>NumberOfNodes</td>\n",
       "      <td>4</td>\n",
       "    </tr>\n",
       "  </tbody>\n",
       "</table>\n",
       "</div>"
      ],
      "text/plain": [
       "                 Key  \\\n",
       "0  ClusterIdentifier   \n",
       "1  NodeType            \n",
       "2  ClusterStatus       \n",
       "3  MasterUsername      \n",
       "4  DBName              \n",
       "5  Endpoint            \n",
       "6  VpcId               \n",
       "7  NumberOfNodes       \n",
       "\n",
       "                                                                                   Value  \n",
       "0  dwhcluster                                                                             \n",
       "1  dc2.large                                                                              \n",
       "2  available                                                                              \n",
       "3  dwhuser                                                                                \n",
       "4  dwh                                                                                    \n",
       "5  {'Address': 'dwhcluster.caerpwnaepms.us-west-2.redshift.amazonaws.com', 'Port': 5439}  \n",
       "6  vpc-0123dc2be9dea8294                                                                  \n",
       "7  4                                                                                      "
      ]
     },
     "execution_count": 8,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "def prettyRedshiftProps(props):\n",
    "    pd.set_option('display.max_colwidth', -1)\n",
    "    keysToShow = [\"ClusterIdentifier\", \"NodeType\", \"ClusterStatus\", \"MasterUsername\", \"DBName\", \"Endpoint\", \"NumberOfNodes\", 'VpcId']\n",
    "    x = [(k, v) for k,v in props.items() if k in keysToShow]\n",
    "    return pd.DataFrame(data=x, columns=[\"Key\", \"Value\"])\n",
    "\n",
    "myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]\n",
    "prettyRedshiftProps(myClusterProps)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DWH_ENDPOINT ::  dwhcluster.caerpwnaepms.us-west-2.redshift.amazonaws.com\n",
      "DWH_ROLE_ARN ::  arn:aws:iam::426847813292:role/dwhRole\n"
     ]
    }
   ],
   "source": [
    "DWH_ENDPOINT = myClusterProps['Endpoint']['Address']\n",
    "DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']\n",
    "print(\"DWH_ENDPOINT :: \", DWH_ENDPOINT)\n",
    "print(\"DWH_ROLE_ARN :: \", DWH_ROLE_ARN)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "ec2.SecurityGroup(id='sg-026397c50ff83f9bb')\n",
      "An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule \"peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW\" already exists\n"
     ]
    }
   ],
   "source": [
    "try:\n",
    "    vpc = ec2.Vpc(id=myClusterProps['VpcId'])\n",
    "    defaultSg = list(vpc.security_groups.all())[0]\n",
    "    print(defaultSg)\n",
    "    \n",
    "    defaultSg.authorize_ingress(\n",
    "        GroupName= defaultSg.group_name,  # TODO: fill out\n",
    "        CidrIp='0.0.0.0/0',  # TODO: fill out\n",
    "        IpProtocol='TCP',  # TODO: fill out\n",
    "        FromPort=int(DWH_PORT),\n",
    "        ToPort=int(DWH_PORT)\n",
    "    )\n",
    "except Exception as e:\n",
    "    print(e)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 10,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "%load_ext sql"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "postgresql://dwhuser:Passw0rd@dwhcluster.caerpwnaepms.us-west-2.redshift.amazonaws.com:5439/dwh\n"
     ]
    },
    {
     "data": {
      "text/plain": [
       "'Connected: dwhuser@dwh'"
      ]
     },
     "execution_count": 11,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "conn_string=\"postgresql://{}:{}@{}:{}/{}\".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)\n",
    "print(conn_string)\n",
    "%sql $conn_string\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 9,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "{'Cluster': {'ClusterIdentifier': 'dwhcluster',\n",
       "  'NodeType': 'dc2.large',\n",
       "  'ClusterStatus': 'deleting',\n",
       "  'MasterUsername': 'dwhuser',\n",
       "  'DBName': 'dwh',\n",
       "  'Endpoint': {'Address': 'dwhcluster.caerpwnaepms.us-west-2.redshift.amazonaws.com',\n",
       "   'Port': 5439},\n",
       "  'ClusterCreateTime': datetime.datetime(2022, 10, 24, 11, 39, 53, 908000, tzinfo=tzlocal()),\n",
       "  'AutomatedSnapshotRetentionPeriod': 1,\n",
       "  'ClusterSecurityGroups': [],\n",
       "  'VpcSecurityGroups': [{'VpcSecurityGroupId': 'sg-026397c50ff83f9bb',\n",
       "    'Status': 'active'}],\n",
       "  'ClusterParameterGroups': [{'ParameterGroupName': 'default.redshift-1.0',\n",
       "    'ParameterApplyStatus': 'in-sync'}],\n",
       "  'ClusterSubnetGroupName': 'default',\n",
       "  'VpcId': 'vpc-0123dc2be9dea8294',\n",
       "  'AvailabilityZone': 'us-west-2d',\n",
       "  'PreferredMaintenanceWindow': 'wed:09:30-wed:10:00',\n",
       "  'PendingModifiedValues': {},\n",
       "  'ClusterVersion': '1.0',\n",
       "  'AllowVersionUpgrade': True,\n",
       "  'NumberOfNodes': 4,\n",
       "  'PubliclyAccessible': True,\n",
       "  'Encrypted': False,\n",
       "  'Tags': [],\n",
       "  'EnhancedVpcRouting': False,\n",
       "  'IamRoles': [{'IamRoleArn': 'arn:aws:iam::426847813292:role/dwhRole',\n",
       "    'ApplyStatus': 'in-sync'}],\n",
       "  'MaintenanceTrackName': 'current'},\n",
       " 'ResponseMetadata': {'RequestId': 'd974fe59-aa81-43f7-bdee-672f4886559b',\n",
       "  'HTTPStatusCode': 200,\n",
       "  'HTTPHeaders': {'x-amzn-requestid': 'd974fe59-aa81-43f7-bdee-672f4886559b',\n",
       "   'content-type': 'text/xml',\n",
       "   'content-length': '2712',\n",
       "   'vary': 'accept-encoding',\n",
       "   'date': 'Mon, 24 Oct 2022 17:06:52 GMT'},\n",
       "  'RetryAttempts': 0}}"
      ]
     },
     "execution_count": 9,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "#### CAREFUL!!\n",
    "#-- Uncomment & run to delete the created resources\n",
    "redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)\n",
    "#### CAREFUL!!\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 11,
   "metadata": {
    "editable": true
   },
   "outputs": [
    {
     "ename": "ClusterNotFoundFault",
     "evalue": "An error occurred (ClusterNotFound) when calling the DescribeClusters operation: Cluster dwhcluster not found.",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mClusterNotFoundFault\u001b[0m                      Traceback (most recent call last)",
      "\u001b[0;32m<ipython-input-11-9b3202a2945e>\u001b[0m in \u001b[0;36m<module>\u001b[0;34m()\u001b[0m\n\u001b[0;32m----> 1\u001b[0;31m \u001b[0mmyClusterProps\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mredshift\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mdescribe_clusters\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mClusterIdentifier\u001b[0m\u001b[0;34m=\u001b[0m\u001b[0mDWH_CLUSTER_IDENTIFIER\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;34m'Clusters'\u001b[0m\u001b[0;34m]\u001b[0m\u001b[0;34m[\u001b[0m\u001b[0;36m0\u001b[0m\u001b[0;34m]\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m      2\u001b[0m \u001b[0mprettyRedshiftProps\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mmyClusterProps\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/opt/conda/lib/python3.6/site-packages/botocore/client.py\u001b[0m in \u001b[0;36m_api_call\u001b[0;34m(self, *args, **kwargs)\u001b[0m\n\u001b[1;32m    318\u001b[0m                     \"%s() only accepts keyword arguments.\" % py_operation_name)\n\u001b[1;32m    319\u001b[0m             \u001b[0;31m# The \"self\" in this scope is referring to the BaseClient.\u001b[0m\u001b[0;34m\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 320\u001b[0;31m             \u001b[0;32mreturn\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m_make_api_call\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0moperation_name\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0mkwargs\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    321\u001b[0m \u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    322\u001b[0m         \u001b[0m_api_call\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0m__name__\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mstr\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mpy_operation_name\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;32m/opt/conda/lib/python3.6/site-packages/botocore/client.py\u001b[0m in \u001b[0;36m_make_api_call\u001b[0;34m(self, operation_name, api_params)\u001b[0m\n\u001b[1;32m    621\u001b[0m             \u001b[0merror_code\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mparsed_response\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mget\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m\"Error\"\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0;34m{\u001b[0m\u001b[0;34m}\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mget\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0;34m\"Code\"\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    622\u001b[0m             \u001b[0merror_class\u001b[0m \u001b[0;34m=\u001b[0m \u001b[0mself\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mexceptions\u001b[0m\u001b[0;34m.\u001b[0m\u001b[0mfrom_code\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0merror_code\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0;32m--> 623\u001b[0;31m             \u001b[0;32mraise\u001b[0m \u001b[0merror_class\u001b[0m\u001b[0;34m(\u001b[0m\u001b[0mparsed_response\u001b[0m\u001b[0;34m,\u001b[0m \u001b[0moperation_name\u001b[0m\u001b[0;34m)\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[0m\u001b[1;32m    624\u001b[0m         \u001b[0;32melse\u001b[0m\u001b[0;34m:\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n\u001b[1;32m    625\u001b[0m             \u001b[0;32mreturn\u001b[0m \u001b[0mparsed_response\u001b[0m\u001b[0;34m\u001b[0m\u001b[0m\n",
      "\u001b[0;31mClusterNotFoundFault\u001b[0m: An error occurred (ClusterNotFound) when calling the DescribeClusters operation: Cluster dwhcluster not found."
     ]
    }
   ],
   "source": [
    "myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]\n",
    "prettyRedshiftProps(myClusterProps)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": [
    "#### CAREFUL!!\n",
    "#-- Uncomment & run to delete the created resources\n",
    "iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn=\"arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess\")\n",
    "iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)\n",
    "#### CAREFUL!!"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "editable": true
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.3"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}