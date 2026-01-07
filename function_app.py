import json
import logging
import os
import boto3
import requests
import azure.functions as func
import jwt

app = func.FunctionApp()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Environment Variables to be set in Azure Function App Configuration:
# AWS_ROLE_ARN=arn:aws:iam::665096241598:role/AzureEventBridgePublisher
# AWS_WEB_IDENTITY_TOKEN_FILE=/var/run/secrets/azure/tokens/azure-identity-token
# AWS_REGION=us-east-1
# EVENT_BUS_NAME=central-event-management-bus
AWS_ROLE_ARN = os.environ["AWS_ROLE_ARN"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
EVENT_BUS = os.environ.get("EVENT_BUS_NAME", "central-event-management-bus")
AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID", "0f4a9737-a4ba-48bd-8174-f54ed1e247e0")

AZURE_APP_ID_URI = f"api://{AZURE_TENANT_ID}/AzureEventBridgePublisher"
API_VERSION = "2019-08-01"

@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger(azeventgrid: func.EventGridEvent):
    logger.info("Received Azure Event Grid event")

    # NOTE: For Activity Logs these are often None
    event_id = azeventgrid.id
    subject = azeventgrid.subject
    event_time = (
        azeventgrid.event_time.isoformat()
        if azeventgrid.event_time
        else None
    )

    data = azeventgrid.get_json()

    # Extract real event type for Activity Logs
    event_type = (
        data.get("operationName")
        or data.get("eventType")
        or "AzureActivityLog"
    )

    logger.info(f"Event ID   : {event_id}")
    logger.info(f"Event Type : {event_type}")
    logger.info(f"Subject    : {subject}")
    logger.info(f"Event Time : {event_time}")
    logger.info(f"Data       : {json.dumps(data)}")

    if event_type is None or event_type.strip() != "Microsoft.Compute/virtualMachines/deallocate/action":
        logger.warning("Event type is None or not the expected type, skipping event publishing")
        return

    event_bus = os.getenv("EVENT_BUS_NAME", "azure-events-bus")

    detail_payload = {
        "id": event_id,
        "eventType": event_type,
        "subject": subject,
        "eventTime": event_time,
        "data": data,
    }

    try:
        # Get Azure-issued OIDC token
        oidc_token = get_azure_msi_token()
        logger.info("Successfully obtained Azure OIDC token", oidc_token)
        claims = jwt.decode(
            oidc_token,
            options={"verify_signature": False}
        )
        logger.info(f"OIDC iss = {claims.get('iss')}")
        logger.info(f"OIDC aud = {claims.get('aud')}")
        # Exchange for AWS credentials
        aws_creds = assume_role_with_oidc(oidc_token)
        eventbridge = boto3.client(
            "events", 
            region_name=AWS_REGION,
            aws_access_key_id=aws_creds["AccessKeyId"],
            aws_secret_access_key=aws_creds["SecretAccessKey"],
            aws_session_token=aws_creds["SessionToken"],
        )
        event_data = {
            "InstanceID": data.get("resourceUri"),
            "Action": data.get("operationName"),
            "Status": data.get("status"),
            "SubscriptionID": data.get("subscriptionId"),
            "TenantID": data.get("tenantId"),
        }
        logger.info(f"Publishing event to EventBridge bus '{event_bus}': {event_data}")
        response = eventbridge.put_events(
            Entries=[
                {
                    "Source": "azure.activitylog",
                    "DetailType": event_type,
                    "Detail": json.dumps(event_data, skipkeys=True),
                    "EventBusName": event_bus,
                }
            ]
        )

        logger.info(f"EventBridge response: {response}")

    except Exception as e:
        # CRITICAL: do NOT raise
        logger.exception("Failed to publish event to AWS EventBridge")
        # Let function succeed so Event Grid does not retry forever
        return

def get_azure_msi_token() -> str:
    """
    Get Azure Managed Identity token using IMDS (Functions-compatible).
    Uses v1 'resource' (NOT scope).
    """
    endpoint = os.environ["IDENTITY_ENDPOINT"]
    header = os.environ["IDENTITY_HEADER"]

    params = {
        "api-version": API_VERSION,
        "resource": AZURE_APP_ID_URI
    }

    headers = {
        "X-IDENTITY-HEADER": header
    }

    resp = requests.get(endpoint, params=params, headers=headers, timeout=10)
    resp.raise_for_status()

    return resp.json()["access_token"]


def assume_role_with_oidc(oidc_token: str) -> dict:
    """
    Exchange Azure OIDC token for AWS temporary credentials
    """
    sts = boto3.client("sts", region_name=AWS_REGION)

    response = sts.assume_role_with_web_identity(
        RoleArn=AWS_ROLE_ARN,
        RoleSessionName="AzureEventGridSession",
        WebIdentityToken=oidc_token,
        DurationSeconds=3600,
    )

    return response["Credentials"]