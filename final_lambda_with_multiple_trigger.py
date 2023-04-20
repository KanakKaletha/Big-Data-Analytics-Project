import boto3
import time

def lambda_handler(event, context):
    
    # Create a session object with your credentials
    session = boto3.Session(
        region_name='us-east-1',
        aws_access_key_id="ASIATJX7AA2GI2KEBF2S",
        aws_secret_access_key="5VDZCns3xXsfaCvKjemM0QMWMCKM7xW62jIh05Md",
        aws_session_token="FwoGZXIvYXdzEDIaDEIdL56YcQxay9NzSSLIAaIUaDWiFzjPQUU6eb3qek7Ft1FzafstwTLHeWV01oP9FVJRbrnGq926HtbctK3879SrlIIdYkXeZnXqyn6UoOVdL2QvUtEew05xF25KBOIRrhEkdbvryiGEej2tRGmLBKBC8XFRy7Bm4fMjt+2FgFN4937j8EjYXc/4wYWWawgrnxZ/Ydc9jjOXcIljQlrP8kXiM3M0TXfqL/JGX33/NPF8fJ0kuPL3LEvwmv4k23fOc2iES3V0gWPnLm1ayCxBVKcDygODTI7nKKTh+qAGMi374WH1AOs+Xp+HdOnPCFS6CgTkrk/knClEn6ZtPjnKbEiy7Hcm4NOEE3DVkHE="
    )

    # Create a CloudFormation client object
    cloudformation = session.client('cloudformation')

    # Set the parameters for your CloudFormation stack
    parameters = [
        {
            "ParameterKey" : 'KeyName',
            "ParameterValue" : 'project_kk_key'
        }
    ]

    # Set the URL of your CloudFormation template
    stack_template = 'https://puttriggerlambda.s3.amazonaws.com/TEMP_SCRIPT/CFT_template.json'

    # Generate a dynamic stack name
    current_time = str(int(time.time()))
    stack_name = "TestingEMRusingPythonS3URL-" + current_time

    # Create the CloudFormation stack
    cloudformation.create_stack(
        StackName=stack_name,
        TemplateURL=stack_template,
        Parameters=parameters
    )

    return {
        'statusCode': 200,
        'body': 'EMR cluster creation initiated by Kanak'
    }
