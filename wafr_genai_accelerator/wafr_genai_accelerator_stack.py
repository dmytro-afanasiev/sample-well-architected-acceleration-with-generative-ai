import os
from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    Tags,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_ec2 as ec2,
    custom_resources as cr,
    aws_s3_deployment as s3deploy,
    aws_dynamodb as dynamodb,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_lambda_event_sources as lambda_event_source,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda_event_sources as lambda_events,
    aws_logs
)

from aws_cdk.aws_ssm import StringParameter
import aws_cdk as cdk
from constructs import Construct
from aws_cdk import Duration
import re

from cdklabs.generative_ai_cdk_constructs import (
    bedrock 
)
import datetime

class WafrGenaiAcceleratorStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, tags: dict = None, **kwargs) -> None:
        """
        Initialize the WAFR GenAI Accelerator Stack.
        
        Args:
            scope: The scope in which to define this construct
            construct_id: The scoped construct ID
            tags: Dictionary of tags to apply to all resources in the stack
            **kwargs: Additional keyword arguments
            
        Raises:
            ValueError: If provided tags are invalid
        """
        super().__init__(scope, construct_id, description="AWS Well-Architected Framework Review (WAFR) Acceleration with Generative AI (GenAI) sample. (uksb-ig1li00ta6)", **kwargs)        
        
        entryTimestampRaw = datetime.datetime.now()
        entryTimestamp = entryTimestampRaw.strftime("%Y%m%d%H%M")
        entryTimestampLabel = entryTimestampRaw.strftime("%Y-%m-%d-%H-%M")      

        # Initialize tags with empty dict if None
        tags = tags or {}
        
        # Apply tags to all resources in the stack
        for key, value in tags.items():
            Tags.of(self).add(key, value)
        
        #Creates Bedrock KB using the generative_ai_cdk_constructs. More info: https://github.com/awslabs/generative-ai-cdk-constructs
        kb = bedrock.KnowledgeBase(self, 'WAFR-KnowledgeBase', 
                    embeddings_model= bedrock.BedrockFoundationModel.TITAN_EMBED_TEXT_V2_1024, 
                    instruction=  'Use this knowledge base to answer questions about AWS Well Architected Framework Review (WAFR).',
                    description= 'This knowledge base contains AWS Well Architected Framework Review (WAFR) reference documents'
                )
        
        KB_ID = kb.knowledge_base_id

        #Create S3 bucket where well architected reference docs are stored
        #S3 bucket for the knowledge base - name of stack followed by well-architected-knowledge-base-analytics
        wafrReferenceDocsBucket = s3.Bucket(self, 
            'wafr-accelerator-kb', 
            bucket_name=f"wafr-accelerator-kb-{entryTimestamp}",
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True)

        WAFR_REFERENCE_DOCS_BUCKET = wafrReferenceDocsBucket.bucket_name

        #Uploading WAFR docs to the corresponding S3 bucket [wafrReferenceDocsBucket]
        s3deploy.BucketDeployment(self, "uploadwellarchitecteddocs",
            sources=[s3deploy.Source.asset('well_architected_docs')],
            destination_bucket=wafrReferenceDocsBucket
        )
        
        #S3 Bucket where customer design is stored
        userUploadBucket = s3.Bucket(self, 
            'wafr-accelerator-upload',
            bucket_name=f"wafr-accelerator-upload-{entryTimestamp}",
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True)
        
        UPLOAD_BUCKET_NAME = userUploadBucket.bucket_name
              
        DEAD_LETTER_QUEUE_UNIQUE_NAME = "wafrAcceleratorDeadLetterQueue-" + entryTimestamp
        WAFR_ACCELERATOR_QUEUE_UNIQUE_NAME = "wafrAcceleratorQueue-" + entryTimestamp
        

        # Create the main queue with a dead-letter queue
        wafrAcceleratorQueue = sqs.Queue(
            self,
            "WAFRAcceleratorQueue",
            queue_name=WAFR_ACCELERATOR_QUEUE_UNIQUE_NAME,
            visibility_timeout=Duration.minutes(20),
            retention_period=Duration.days(4),
            delivery_delay=Duration.seconds(5),
            encryption=sqs.QueueEncryption.KMS_MANAGED,  # Use the AWS-managed KMS key for SQS
            enforce_ssl=True
        )
        
        #Create DynamoDB table for tracking WAFR accelerator runs
        wafrRunsTable = dynamodb.TableV2(self, "review-runs",
            table_name=f"wafr-reviewruns-{entryTimestamp}",
            partition_key=dynamodb.Attribute(
                name="analysis_id", type=dynamodb.AttributeType.STRING),
                sort_key=dynamodb.Attribute(
                    name="analysis_submitter", type=dynamodb.AttributeType.STRING),
            billing=dynamodb.Billing.on_demand(),
            removal_policy=RemovalPolicy.DESTROY
        )
        
                                
        WAFR_RUNS_TABLE = wafrRunsTable.table_name

        #Adds the created S3 bucket [docBucket] as a Data Source for Bedrock KB
        kbDataSource = bedrock.S3DataSource(self, 'DataSource',
            bucket= wafrReferenceDocsBucket,
            knowledge_base=kb,
            data_source_name='wafr-reference-docs',
            chunking_strategy = bedrock.ChunkingStrategy.FIXED_SIZE,
            max_tokens=500,
            overlap_percentage=20
        )
        
        # Data Ingestion Params
        dataSourceIngestionParams = {
            "dataSourceId": kbDataSource.data_source_id,
            "knowledgeBaseId": KB_ID,
        }
        
         # Define a custom resource to make an AwsSdk startIngestionJob call. This will do an initial sync of the S3 bucket [docBucket].    
        ingestion_job_cr = cr.AwsCustomResource(self, "IngestionCustomResource",
            on_create=cr.AwsSdkCall(
                service="bedrock-agent",
                action="startIngestionJob",
                parameters=dataSourceIngestionParams,
                physical_resource_id=cr.PhysicalResourceId.of("Parameter.ARN")
                ),
                policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                    resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
                )
        )
        
        #Create DynamoDB table for tracking WAFR accelerator runs 
        wafrPillarQuestionPromptsTable = dynamodb.TableV2(self, "wafr-pillar-question-prompts",
            table_name=f"wafr-pillar-question-prompts-{entryTimestamp}",
            partition_key=dynamodb.Attribute(
                name="wafr_lens", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(
                name="wafr_pillar", type=dynamodb.AttributeType.STRING),
            billing=dynamodb.Billing.on_demand(),
            removal_policy=RemovalPolicy.DESTROY
            )
        
        WAFR_PILLAR_QUESTIONS_PROMPT_TABLE = wafrPillarQuestionPromptsTable.table_name

        existing_permissions_boundary = iam.Role.from_role_name(
            self, 'ExistingPermissionsBoundary', role_name=os.getenv('WAFR_ROLE_PERMISSIONS_BOUNDARY_NAME')
        )
        
        # Create an IAM role for the insertWafrPromptsFunctionRole Lambda function
        insertWafrPromptsFunctionRole = iam.Role(
            self, "LambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            permissions_boundary=existing_permissions_boundary
        )
        
        insertWafrPromptsFunction = _lambda.Function(self, "insertWAFRPrompts",
            runtime=_lambda.Runtime.PYTHON_3_12,
            code = _lambda.Code.from_asset("lambda_dir/insert_wafr_prompts"), 
            handler="insert_wafr_prompts.lambda_handler",
            timeout=cdk.Duration.seconds(30),
            memory_size=128,
            environment={
                "DD_TABLE_NAME": WAFR_PILLAR_QUESTIONS_PROMPT_TABLE,
                "REGION_NAME": Stack.of(self).region
            },
            role = insertWafrPromptsFunctionRole
        )
        
        wafrPillarQuestionPromptsTable.grant_write_data(insertWafrPromptsFunction)
        wafrPillarQuestionPromptsTable.grant_read_data(insertWafrPromptsFunction)
        
        promptsBucket = s3.Bucket(self, 'wafr-prompts',
            bucket_name=f"wafr-prompts-{entryTimestamp}", 
            removal_policy=RemovalPolicy.DESTROY, 
            enforce_ssl=True,
            auto_delete_objects=True)
            
        promptsBucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(insertWafrPromptsFunction)
        )
        promptsBucket.grant_put(insertWafrPromptsFunction)
        promptsBucket.grant_read(insertWafrPromptsFunction)

        #Upload bucket for Uploading WAFR docs to the corresponding S3 bucket [docBucket]
        s3deploy.BucketDeployment(self, "promptsBucketDeploy",
            sources=[s3deploy.Source.asset('wafr-prompts')],
            destination_bucket=promptsBucket
        )
        
        # Create VPC
        existing_vpc = ec2.Vpc.from_lookup(
            self, 'ExistingVpc', vpc_id=os.getenv('WAFR_VPC_ID')
        )

        # Create Security Group
        existing_security_group = ec2.SecurityGroup.from_security_group_id(
            self, 'ExistingSecurityGroup', os.getenv('WAFR_SG_ID'), mutable=False
        )

    
        # Create IAM role for EC2 instance
        ec2Role = iam.Role(self, "StreamlitAppRole-" + entryTimestamp,
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore")
            ],
            inline_policies={
                "ec2RolePolicies": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem", "dynamodb:Scan", "dynamodb:Query"],
                            resources=[
                                wafrPillarQuestionPromptsTable.table_arn,
                                wafrRunsTable.table_arn
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": self.account
                                }
                            },
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "s3:PutObject",
                                "s3:GetObject",
                                "s3:ListBucket"
                            ],
                            resources=[
                                f"arn:aws:s3:::wafr-prompts-{entryTimestamp}/*",
                                f"arn:aws:s3:::wafr-accelerator-ui-{entryTimestamp}",
                                f"arn:aws:s3:::wafr-accelerator-ui-{entryTimestamp}/*",
                                f"arn:aws:s3:::wafr-accelerator-upload-{entryTimestamp}/*"
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": self.account
                                }
                            },
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "bedrock:InvokeModel",
                                "bedrock:InvokeModelWithResponseStream"
                            ],
                            resources=[
                                f"arn:aws:bedrock:{self.region}::foundation-model/anthropic.claude-3-5-sonnet-20240620-v1:0"
                            ],
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "bedrock:Retrieve"
                            ],
                            resources=[
                                f"arn:aws:bedrock:{self.region}:{self.account}:knowledge-base/{KB_ID}",
                                f"arn:aws:bedrock:{self.region}:{self.account}:knowledge-base/{KB_ID}/*"
                            ],
                            effect=iam.Effect.ALLOW
                        ),
                        
                        iam.PolicyStatement(
                            actions=[
                                "sqs:SendMessage",
                                "sqs:ReceiveMessage",
                                "sqs:DeleteMessage",
                                "sqs:GetQueueAttributes",
                                "sqs:GetQueueUrl"
                            ],
                            resources=[
                                f"arn:aws:sqs:{self.region}:{self.account}:{DEAD_LETTER_QUEUE_UNIQUE_NAME}",
                                f"arn:aws:sqs:{self.region}:{self.account}:{WAFR_ACCELERATOR_QUEUE_UNIQUE_NAME}"
                            ],
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "ssm:GetParameter",
                                "ssm:GetParameters",
                                "ssm:GetParametersByPath",
                                "ssm:PutParameter",
                                "ssm:DeleteParameter",
                                "ssm:DeleteParameters",
                                "ssm:DescribeParameters",
                                "ssm:LabelParameterVersion"
                            ],
                            resources=[f"arn:aws:ssm:{self.region}:{self.account}:parameter/wafr-accelerator/*"]
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "textract:StartDocumentAnalysis",
                                "textract:StartDocumentTextDetection",
                                "textract:GetDocumentAnalysis",
                                "textract:GetDocumentTextDetection"
                            ],
                            resources=["*"]  
                        ),
                        iam.PolicyStatement(
                        actions=[
                            "wellarchitected:CreateWorkload",
                            "wellarchitected:UpdateWorkload",
                            "wellarchitected:UpdateAnswer",
                            "wellarchitected:GetAnswer",
                            "wellarchitected:ListAnswers",
                            "wellarchitected:ListLensReviewImprovements",
                            "wellarchitected:ListWorkloads"
                        ],
                        resources=["*"]  
                        )
                    ]
                )
            },
            permissions_boundary=existing_permissions_boundary
        )
        
        #Reading user_data_script.sh file which contains the linux commands that must be run when the EC2 boots up.
        with open("user_data_script.sh", "r", encoding='UTF-8') as f:
            user_data_script = f.read()
        
        user_data_script = re.sub(r'{{REGION}}', Stack.of(self).region, user_data_script)
  
        ec2_create = ec2.Instance(self, "StreamlitAppInstance-" + entryTimestamp,
            instance_type=ec2.InstanceType("t2.micro"),
            machine_image=ec2.AmazonLinuxImage(generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2023),
            vpc=existing_vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            security_group=existing_security_group,
            role=ec2Role,
            associate_public_ip_address=True,
            user_data=ec2.UserData.custom(user_data_script),
            block_devices=[
                ec2.BlockDevice(
                    device_name="/dev/xvda",
                    volume=ec2.BlockDeviceVolume.ebs(
                        volume_size=8,  # Size in GB
                        encrypted=True,
                        delete_on_termination=True,  # Optional: delete the volume when the instance is terminated
                    )
                )
            ],
            # This will propagate instance tags to volumes
            propagate_tags_to_volume_on_creation=True
        )

        EC2_INSTANCE_ID = ec2_create.instance_id

        #Print the Cloudfront Public Domain Name after CDK Deployment for easier access
        CfnOutput(
            self, "FrontEnd-EC2-Instance-Id",
            value=EC2_INSTANCE_ID,
            description="Front end UI EC2 instance id created at : " + entryTimestampLabel
        )

        uiPage1UpdateParameter = StringParameter(
            self, "uiPage1UpdateParameter-" + entryTimestamp,
            parameter_name="/wafr-accelerator/" + entryTimestamp + "/1_New_WAFR_Review-updated",
            string_value="False",
            description="1_New_WAFR_Review-updated status created at : " + entryTimestampLabel
        )
        
        PARAMETER_1_NEW_WAFR_REVIEW = uiPage1UpdateParameter.parameter_name
        
        uiPage2UpdateParameter = StringParameter(
            self, "uiPage2UpdateParameter-" + entryTimestamp,
            parameter_name="/wafr-accelerator/" + entryTimestamp + "/2_Existing_WAFR_Reviews-updated",
            string_value="False",
            description="2_Existing_WAFR_Reviews-updated status created at : " + entryTimestampLabel
        )
        
        PARAMETER_2_EXISTING_WAFR_REVIEWS = uiPage2UpdateParameter.parameter_name
        
        uiSyncFlagParameter = StringParameter(
            self, "uiSyncFlagParameter-" + entryTimestamp,
            parameter_name="/wafr-accelerator/" + entryTimestamp + "/uiSyncInitiatedFlag",
            string_value="False",
            description="uiSyncInitiatedFlag created at : " + entryTimestampLabel
        )
        
        PARAMETER_UI_SYNC_INITAITED_FLAG = uiSyncFlagParameter.parameter_name
        
        uiLoginPageParameter = StringParameter(
            self, "uiLoginPageParameter-" + entryTimestamp,
            parameter_name="/wafr-accelerator/" + entryTimestamp + "/uiLoginPageParameter",
            string_value="False",
            description="uiLoginPageParameter-updated status created at : " + entryTimestampLabel
        )
        
        PARAMETER_3_LOGIN_PAGE = uiLoginPageParameter.parameter_name
    
        
        # bucket for ui source code
        wafrUIBucket = s3.Bucket(self, 
            'wafr-accelerator-ui', 
            bucket_name=f"wafr-accelerator-ui-{entryTimestamp}", 
            removal_policy=RemovalPolicy.DESTROY, 
            enforce_ssl=True,
            auto_delete_objects=True)
        
        # Create an IAM role for the replaceUITokensFunctionRole Lambda function
        replaceUITokensFunctionRole = iam.Role(
            self, "replaceUITokensFunctionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            inline_policies={
                "replaceUITokensFunctionRolePolicies": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "s3:PutObject",
                                "s3:GetObject"
                            ],
                            resources=[
                                f"{wafrUIBucket.bucket_arn}/*"
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": self.account
                                }
                            },
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "ssm:GetParameter",
                                "ssm:GetParameters",
                                "ssm:GetParametersByPath",
                                "ssm:PutParameter",
                                "ssm:DeleteParameter",
                                "ssm:DeleteParameters",
                                "ssm:DescribeParameters",
                                "ssm:LabelParameterVersion"
                            ],
                            resources=[f"arn:aws:ssm:{self.region}:{self.account}:parameter/wafr-accelerator/*"]
                        ),
                         iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "ssm:SendCommand",
                            ],
                            resources=[
                                f"arn:aws:ssm:{self.region}::document/AWS-RunShellScript",
                                f"arn:aws:ec2:{self.region}:{self.account}:instance/{EC2_INSTANCE_ID}"
                            ]
                        ),
                         iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "ssm:GetCommandInvocation" 
                            ],
                            resources=[
                                f"arn:aws:ssm:{self.region}:{self.account}:*"
                            ]
                        )
                    ]
                )
            },
            permissions_boundary=existing_permissions_boundary
        )
        
        replaceUITokensFunction = _lambda.Function(self, "replaceUITokensFunction",
            runtime=_lambda.Runtime.PYTHON_3_12,
            code = _lambda.Code.from_asset("lambda_dir/replace_ui_tokens"), # Points to the lambda directory
            handler="replace_ui_tokens.lambda_handler",
            timeout=cdk.Duration.seconds(300),
            memory_size=128,
            environment={
                "WAFR_ACCELERATOR_QUEUE_URL": wafrAcceleratorQueue.queue_url,
                "WAFR_UI_BUCKET_NAME": wafrUIBucket.bucket_name,
                "WAFR_UI_BUCKET_ARN": wafrUIBucket.bucket_arn,
                "REGION_NAME": Stack.of(self).region,
                "WAFR_RUNS_TABLE": wafrRunsTable.table_name,
                "EC2_INSTANCE_ID": EC2_INSTANCE_ID,
                "UPLOAD_BUCKET_NAME" : UPLOAD_BUCKET_NAME,
                "PARAMETER_2_EXISTING_WAFR_REVIEWS" : PARAMETER_2_EXISTING_WAFR_REVIEWS,
                "PARAMETER_1_NEW_WAFR_REVIEW" : PARAMETER_1_NEW_WAFR_REVIEW,
                "PARAMETER_UI_SYNC_INITAITED_FLAG" : PARAMETER_UI_SYNC_INITAITED_FLAG,
                "PARAMETER_3_LOGIN_PAGE" : PARAMETER_3_LOGIN_PAGE, 
                "PARAMETER_COGNITO_USER_POOL_ID" : 'mocked_cognito_user_pool_id' ,
                "PARAMETER_COGNITO_USER_POOL_CLIENT_ID" : 'mocked_cognito_user_pool_name',
            },
            role = replaceUITokensFunctionRole,
            events=[lambda_events.S3EventSource(bucket=wafrUIBucket, events=[s3.EventType.OBJECT_CREATED], filters=[s3.NotificationKeyFilter(prefix="tokenized-pages/", suffix=".py")])]
        )
                    
        #Uploading UI code to the corresponding S3 bucket [wafrReferenceDocsBucket]
        wafrUIBucketDeploy = s3deploy.BucketDeployment(self, "uploaduicode",
            sources=[s3deploy.Source.asset('ui_code')],
            destination_bucket=wafrUIBucket
        )
               
        # Create an IAM role for the startWafrReviewFunctionRole Lambda function
        startWafrReviewFunctionRole = iam.Role(
            self, "startWafrReviewFunctionRoleLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            inline_policies={
                "startWafrReviewFunctionRolePolicies": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem", "dynamodb:Scan", "dynamodb:Query"],
                            resources=[
                                wafrPillarQuestionPromptsTable.table_arn,
                                wafrRunsTable.table_arn
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": self.account
                                }
                            },
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "s3:PutObject",
                                "s3:GetObject",
                                "s3:DeleteObject"
                            ],
                            resources=[
                                f"arn:aws:s3:::wafr-prompts-{entryTimestamp}/*",
                                f"arn:aws:s3:::wafr-accelerator-upload-{entryTimestamp}/*"
                            ],
                            conditions={
                                "StringEquals": {
                                    "aws:ResourceAccount": self.account
                                }
                            },
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "bedrock:InvokeModel",
                                "bedrock:InvokeModelWithResponseStream"
                            ],
                            resources=[
                                f"arn:aws:bedrock:{self.region}::foundation-model/anthropic.claude-3-5-sonnet-20240620-v1:0"
                            ],
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "bedrock:Retrieve"
                            ],
                            resources=[
                                f"arn:aws:bedrock:{self.region}:{self.account}:knowledge-base/{KB_ID}",
                                f"arn:aws:bedrock:{self.region}:{self.account}:knowledge-base/{KB_ID}/*"
                            ],
                            effect=iam.Effect.ALLOW
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "sqs:SendMessage",
                                "sqs:ReceiveMessage",
                                "sqs:DeleteMessage",
                                "sqs:GetQueueAttributes",
                                "sqs:GetQueueUrl"
                            ],
                            resources=[
                                f"arn:aws:sqs:{self.region}:{self.account}:{DEAD_LETTER_QUEUE_UNIQUE_NAME}",
                                f"arn:aws:sqs:{self.region}:{self.account}:{WAFR_ACCELERATOR_QUEUE_UNIQUE_NAME}"
                            ]
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "states:StartExecution",
                                "states:DescribeExecution",
                                "states:GetExecutionHistory"
                            ],
                            resources=[f"arn:aws:states:{self.region}:{self.account}:stateMachine:WAFRReviewStateMachine-{entryTimestamp}"]
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "textract:StartDocumentAnalysis",
                                "textract:StartDocumentTextDetection",
                                "textract:GetDocumentAnalysis",
                                "textract:GetDocumentTextDetection"
                            ],
                            resources=["*"]  
                        ),
                        iam.PolicyStatement(
                            actions=[
                                "wellarchitected:CreateWorkload",
                                "wellarchitected:UpdateWorkload",
                                "wellarchitected:UpdateAnswer",
                                "wellarchitected:GetAnswer",
                                "wellarchitected:ListAnswers",
                                "wellarchitected:ListLensReviewImprovements",
                                "wellarchitected:ListWorkloads",
                                "wellarchitected:GetLensReview",
                                "wellarchitected:CreateMilestone"
                            ],
                            resources=["*"]  
                        )
                    ]
                )
            },
            permissions_boundary=existing_permissions_boundary
        )
        
        #Define Lambda functions
        prepare_wafr_review = _lambda.Function(self, "prepare_wafr_review",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="prepare_wafr_review.lambda_handler",
            code=_lambda.Code.from_asset("lambda_dir/prepare_wafr_review"),
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
            environment={
                "KNOWLEDGE_BASE_ID": KB_ID,
                "LLM_MODEL_ID": "anthropic.claude-3-5-sonnet-20240620-v1:0",
                "REGION": Stack.of(self).region, 
                "UPLOAD_BUCKET_NAME": userUploadBucket.bucket_name,
                "WAFR_ACCELERATOR_RUNS_DD_TABLE_NAME": WAFR_RUNS_TABLE,
                "WAFR_PROMPT_DD_TABLE_NAME": WAFR_PILLAR_QUESTIONS_PROMPT_TABLE,
                "BEDROCK_SLEEP_DURATION" : "60",
                "BEDROCK_MAX_TRIES" : "5"
            },
            role = startWafrReviewFunctionRole,
            reserved_concurrent_executions=1
        )
        extract_document_text = _lambda.Function(self, "extract_document_text",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="extract_document_text.lambda_handler",
            code=_lambda.Code.from_asset("lambda_dir/extract_document_text"),
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            role = startWafrReviewFunctionRole,
            reserved_concurrent_executions=1
        )
        generate_solution_summary = _lambda.Function(self, "generate_solution_summary",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="generate_solution_summary.lambda_handler",
            code=_lambda.Code.from_asset("lambda_dir/generate_solution_summary"),
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            role = startWafrReviewFunctionRole,
            reserved_concurrent_executions=1
        )
        generate_prompts = _lambda.Function(self, "generate_prompts_for_all_the_selected_pillars",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="generate_prompts_for_all_the_selected_pillars.lambda_handler",
            code=_lambda.Code.from_asset("lambda_dir/generate_prompts_for_all_the_selected_pillars"),
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            role = startWafrReviewFunctionRole,
            reserved_concurrent_executions=1,
            environment={
                "WAFR_REFERENCE_DOCS_BUCKET" : WAFR_REFERENCE_DOCS_BUCKET
            }
        )
        generate_pillar_question_response = _lambda.Function(self, "generate_pillar_question_response",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="generate_pillar_question_response.lambda_handler",
            code=_lambda.Code.from_asset("lambda_dir/generate_pillar_question_response"),
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            role = startWafrReviewFunctionRole,
            reserved_concurrent_executions=1,
            environment={
                "BEDROCK_SLEEP_DURATION" : "60",
                "BEDROCK_MAX_TRIES" : "5"
            }
        )
        update_review_status = _lambda.Function(self, "update_review_status",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="update_review_status.lambda_handler",
            code=_lambda.Code.from_asset("lambda_dir/update_review_status"),
            timeout=cdk.Duration.minutes(15),
            memory_size=256,
            role = startWafrReviewFunctionRole,
            reserved_concurrent_executions=1
        )

        # Create an IAM role for the Step Function
        step_function_role = iam.Role(
            self, "WAFRStepFunctionRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
            inline_policies={
                "StepFunctionRolePolicies": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents"
                            ],
                            resources=[f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/vendedlogs/states/*"],
                            effect=iam.Effect.ALLOW
                        )
                    ]
                )
            },
            permissions_boundary=existing_permissions_boundary
        )

        # Grant the Step Function role permission to invoke the Lambda functions
        prepare_wafr_review.grant_invoke(step_function_role)
        extract_document_text.grant_invoke(step_function_role)
        generate_solution_summary.grant_invoke(step_function_role)
        generate_prompts.grant_invoke(step_function_role)
        generate_pillar_question_response.grant_invoke(step_function_role)
        update_review_status.grant_invoke(step_function_role)
        
        # Define Step Function tasks
        pass_state = sfn.Pass(
            self, "Pass",
            result=sfn.Result.from_object({"InitializeWAFRReview": True}),
            result_path=sfn.JsonPath.DISCARD
        )

        prepare_wafr_review_task = tasks.LambdaInvoke(
            self, "Prepare WAFR review",
            lambda_function=prepare_wafr_review,
            output_path="$.Payload.body"
        )
        extract_document_text_task = tasks.LambdaInvoke(
            self, "Extract document text",
            lambda_function=extract_document_text,
            output_path="$.Payload.body"
        )
        generate_solution_summary_task = tasks.LambdaInvoke(
            self, "Generate solution summary",
            lambda_function=generate_solution_summary,
            output_path="$.Payload.body"
        )
        generate_prompts_task = tasks.LambdaInvoke(
            self, "Generate prompts for selected pillars",
            lambda_function=generate_prompts,
            output_path="$.Payload.body"
        )
        generate_pillar_question_response_task = tasks.LambdaInvoke(
            self, "Generate pillar question response",
            lambda_function=generate_pillar_question_response,
            output_path="$.Payload.body"
        )
        update_review_status_task = tasks.LambdaInvoke(
            self, "Mark review as complete",
            lambda_function=update_review_status,
            output_path="$.Payload"
        )

        wait_state = sfn.Wait(
            self, "Wait", 
            time=sfn.WaitTime.duration(cdk.Duration.seconds(40))
        )
     
        # Define the Map state
        map_state = sfn.Map(
            self, "Loop through selected pillars",
            max_concurrency=1,
            items_path="$.all_pillar_prompts"
        )
        
        # Define the iterator chain
        iterator_chain = sfn.Chain \
            .start(wait_state) \
            .next(generate_pillar_question_response_task)
        
        # Set the iterator
        map_state.iterator(iterator_chain)

        # Create a log group for the Step Function
        wafr_stepmachine_log_group = aws_logs.LogGroup(
            self, "WAFRReviewStateMachineLogGroup",
            retention=aws_logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY 
        )

        # Define the chain of states
        chain = sfn.Chain \
            .start(pass_state) \
            .next(prepare_wafr_review_task) \
            .next(extract_document_text_task) \
            .next(generate_solution_summary_task) \
            .next(generate_prompts_task) \
            .next(map_state) \
            .next(update_review_status_task)

        # Create the state machine using definitionBody instead of definition
        state_machine = sfn.StateMachine(
            self, "WAFRReviewStateMachine",
            state_machine_name=f"WAFRReviewStateMachine-{entryTimestamp}",
            removal_policy=RemovalPolicy.DESTROY,
            definition_body=sfn.DefinitionBody.from_chainable(chain),
            timeout=cdk.Duration.seconds(6000),
            role=step_function_role,
            tracing_enabled=True,
            logs=sfn.LogOptions(
                destination=wafr_stepmachine_log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=False
            )
        )        
        
        startWafrReviewFunction = _lambda.Function(self, "startWafrReview",
            runtime=_lambda.Runtime.PYTHON_3_12,
            code = _lambda.Code.from_asset("lambda_dir/start_wafr_review"), # Points to the lambda directory
            handler="start_wafr_review.lambda_handler",
            timeout=cdk.Duration.minutes(15),
            memory_size=512,
            environment={
                "KNOWLEDGE_BASE_ID": KB_ID,
                "LLM_MODEL_ID": "anthropic.claude-3-5-sonnet-20240620-v1:0",
                "REGION": Stack.of(self).region,
                "UPLOAD_BUCKET_NAME": userUploadBucket.bucket_name,
                "WAFR_ACCELERATOR_RUNS_DD_TABLE_NAME": WAFR_RUNS_TABLE,
                "WAFR_PROMPT_DD_TABLE_NAME": WAFR_PILLAR_QUESTIONS_PROMPT_TABLE,
                "START_WAFR_REVIEW_STATEMACHINE_ARN": state_machine.state_machine_arn,
                "BEDROCK_SLEEP_DURATION" : "60",
                "BEDROCK_MAX_TRIES" : "5",
                "WAFR_REFERENCE_DOCS_BUCKET" : WAFR_REFERENCE_DOCS_BUCKET
            },
            role = startWafrReviewFunctionRole,
            reserved_concurrent_executions=1
        )

        wafrPillarQuestionPromptsTable.grant_write_data(startWafrReviewFunction)
        wafrRunsTable.grant_write_data(startWafrReviewFunction)
        
        # Grant the Lambda function permission to access the SQS queue
        wafrAcceleratorQueue.grant_consume_messages(startWafrReviewFunction)
        
        sqs_event_source = lambda_event_source.SqsEventSource(wafrAcceleratorQueue, batch_size=1 )#, maximum_concurrency = 2 )
        
        # Create the SQS event source with maximum concurrency set to 2
        startWafrReviewFunction.add_event_source(sqs_event_source)
        
        # # ------------ Node dependencies ---------------------
        kbDataSource.node.add_dependency(wafrReferenceDocsBucket)
        ingestion_job_cr.node.add_dependency(kb)
        
        ec2_create.node.add_dependency(kb)

        ec2_create.node.add_dependency(ec2Role)

        wafrUIBucketDeploy.node.add_dependency(replaceUITokensFunction)
        
        wafrUIBucketDeploy.node.add_dependency(ec2_create)
        
        startWafrReviewFunction.node.add_dependency(state_machine)
        startWafrReviewFunction.node.add_dependency(ec2_create)
        
