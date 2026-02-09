import boto3
import os
import csv
import io
import html
import logging
import sys
from collections import Counter
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger():
    logger = logging.getLogger("mw_pre_patch")
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    log_file = os.path.join(
        LOG_DIR,
        f"mw_pre_patch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

logger = setup_logger()

#1. to read account ids, role_name and region from local csv file
def read_csv_local(file_name):
    logger.info(f"Reading accounts from CSV: {file_name}")
    rows = []
    try:
        file_path = os.path.join(os.path.dirname(__file__), file_name)

        with open(file_path, mode="r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
        logger.info(f"Loaded {len(rows)} account rows")
    except Exception:
        logger.error("Failed to read accounts CSV", exc_info=True)
        raise
    return rows

#2. checks for Maintenance windows scheduled for current date
def runs_today(next_execution_time):
    logger.debug(f"Evaluating NextExecutionTime: {next_execution_time}")

    try:
        #  Normalize to datetime
        if isinstance(next_execution_time, str):
            next_execution_time = datetime.fromisoformat(
                next_execution_time.replace("Z", "+00:00")
            )

        #  Ensure timezone-aware
        if next_execution_time.tzinfo is None:
            next_execution_time = next_execution_time.replace(tzinfo=timezone.utc)

        #  Today's UTC window
        now_utc = datetime.now(timezone.utc)
        start_of_today = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_today = start_of_today + timedelta(days=1)

        # print(f"NextExecutionTime: {next_execution_time}")
        # print(f"UTC Window: {start_of_today} → {end_of_today}")

        result = start_of_today <= next_execution_time < end_of_today
        logger.debug(f"runs_today result: {result}")
        return result

    except Exception:
        logger.error("Error while evaluating runs_today()", exc_info=True)
        return False

# 3.Gets the target instance count per Maintenance window
def get_target_count(ssm, ec2, resourcegroups, window_id):
    logger.info(f"Resolving target count for MW {window_id}")

    try:
        targets_response = ssm.describe_maintenance_window_targets(
            WindowId=window_id
        )

        total = 0

        for mw_target in targets_response["Targets"]:

            # Collect tag filters PER MW TARGET
            tag_filters = []

            for rule in mw_target.get("Targets", []):
                key = rule["Key"]
                values = rule["Values"]

                # Case 1: Explicit Instance IDs
                if key == "InstanceIds":
                    logger.debug(f"InstanceIds target: {values}")
                    total += len(values)

                # Case 2: Tag-based targets
                elif key.startswith("tag:"):
                    tag_key = key.split("tag:")[1]
                    tag_filters.append({
                        "Name": f"tag:{tag_key}",
                        "Values": values
                    })

                # Case 3: Resource Groups
                elif key == "resource-groups:Name":
                    logger.debug(f"Resolving Resource Group: {group_name}")
                    for group_name in values:

                        paginator = resourcegroups.get_paginator(
                            "list_group_resources"
                        )

                        count = 0
                        for page in paginator.paginate(Group=group_name):
                            for resource in page["ResourceIdentifiers"]:
                                # Only count EC2 instances
                                if resource["ResourceType"] == "AWS::EC2::Instance":
                                    count += 1
                        logger.debug(f"Resource Group {group_name} resolved to {count}")
                        total += count

                else:
                    logger.warning(f"Unsupported target rule: {key}")
                    print(f"Unsupported target rule: {key}")

            # Resolve tag-based targets for THIS MW TARGET ONLY
            if tag_filters:
                paginator = ec2.get_paginator("describe_instances")
                instance_ids = set()

                for page in paginator.paginate(Filters=tag_filters):
                    for reservation in page["Reservations"]:
                        for instance in reservation["Instances"]:
                            instance_ids.add(instance["InstanceId"])
                logger.debug(f"Tag-based EC2 instances resolved: {len(instance_ids)}")
                total += len(instance_ids)

        #print(f"Resolved target count = {total}")
        logger.info(f"Target count for MW {window_id}: {total}")
        return total
    
    except Exception:
        logger.error(
            f"Failed resolving target count for MW {window_id}",
            exc_info=True
        )
        return 0
    
#4.Write CSV to S3
def write_csv_to_s3(s3, bucket, key, rows):
    logger.info(f"Writing CSV to s3://{bucket}/{key}")
    try:
        buffer = io.StringIO()
        fieldnames = rows[0].keys()

        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue()
        )
        #print(f"data is written to s3 bucket")
        logger.info("CSV successfully written to S3")
    except Exception:
        logger.error("Failed writing CSV to S3", exc_info=True)
        raise

#5. To create html file
def build_html_table(output_rows):
    logger.info("Building HTML email body")
    if not output_rows:
        logger.warning("No output rows for HTML table")
        return """
        <html>
          <body>
            <p>No Maintenance Windows running today.</p>
          </body>
        </html>
        """

    # Exclude MaintenanceWindowId field
    headers = [h for h in output_rows[0].keys() if h != "MaintenanceWindowId"]

    def prettify_header(header):
        return "".join(
            f" {c}" if c.isupper() else c for c in header
        ).strip()

    # To count how many rows each AccountId has
    account_counts = Counter(row["AccountId"] for row in output_rows)

    html_body = """
    <html>
    <body>
      <p>Hello Team,</p>
      <p>Below are the Maintenance Windows running today:</p>

      <table border="1" cellpadding="6" cellspacing="0"
             style="border-collapse:collapse; font-family:Arial, sans-serif; font-size:13px;">
        <tr style="background-color:#f2f2f2; font-weight:bold;">
    """

    # Header row
    for header in headers:
        html_body += f"<th>{html.escape(prettify_header(header))}</th>"

    html_body += "</tr>"

    rendered_accounts = set()

    for row in output_rows:
        html_body += "<tr>"

        for header in headers:
            value = row.get(header, "")

            # Proper merge using rowspan
            if header == "AccountId":
                if value not in rendered_accounts:
                    rowspan = account_counts[value]
                    html_body += (
                        f"<td rowspan='{rowspan}' "
                        f"style='vertical-align:middle;'>"
                        f"{html.escape(str(value))}</td>"
                    )
                    rendered_accounts.add(value)
                # else: DO NOT render AccountId cell at all
            else:
                html_body += f"<td>{html.escape(str(value))}</td>"

        html_body += "</tr>"

    html_body += """
      </table>

      <br>
      <p>Regards,<br>Patch Automation</p>
    </body>
    </html>
    """

    return html_body

#6.loads account id details from which email will be sent
def load_email_config(csv_file):
    logger.info(f"Loading email config from {csv_file}")
    try:
        with open(csv_file, newline="") as f:
            reader = csv.DictReader(f)
            row = next(reader)   
            return {
                "account_id": row["account_id"],
                "role_name": row["role_name"],
                "region": row["region"]
            }
    except Exception:
        logger.error("Failed loading email config", exc_info=True)
        raise
#7. Assume role into target account
def assume_role(account_id, role_name, region):
    logger.info(f"Assuming role {role_name} in account {account_id}")
    try:
        sts = boto3.client("sts")

        role_arn = f"arn:aws:iam::{account_id}:role/{role_name}"

        response = sts.assume_role(
            RoleArn=role_arn,
            RoleSessionName="MWCheckSession"
        )

        creds = response["Credentials"]
        return boto3.Session(
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=region
        )
    except Exception:
        logger.error("AssumeRole failed", exc_info=True)
        raise    

# def assume_role(account_id, role_name, region):
#     """
#     Uses local AWS credentials.
#     account_id and role_name are intentionally ignored.
#     """
#     logger.info(
#         f"Creating session using local credentials "
#         f"(account_id={account_id}, role_name={role_name}, region={region})"
#     )    
#     return boto3.Session(region_name=region)


#8.SES
def send_email_ses(session, subject, html_body, sender, recipients, region):
    logger.info(f"Sending SES email to {recipients}")
    try:
        ses = session.client("ses", region_name=region)
        #ses = boto3.client("ses", region_name=region)

        response = ses.send_email(
            Source=sender,
            Destination={
                "ToAddresses": recipients
            },
            Message={
                "Subject": {
                    "Data": subject,
                    "Charset": "UTF-8"
                },
                "Body": {
                    "Html": {
                        "Data": html_body,
                        "Charset": "UTF-8"
                    }
                }
            }
        )
        logger.info(f"SES email sent successfully. MessageId={response['MessageId']}")
        return response
    except Exception:
        logger.error("SES email send failed", exc_info=True)
        raise

# Main function
def main():
    logger.info("Starting Maintenance Window pre-patch scan")

    OUTPUT_BUCKET = "mmpatching-custom-patchbaseline-dev"
    OUTPUT_KEY = "pre_patch_notification/mw-running-today-output.csv"

    EMAIL_FROM = "abhishek.jha@modmed.com"
    EMAIL_TO = ["abhishek.jha@modmed.com"]
    EMAIL_REGION = "us-east-1"

    pre_patch_rows = read_csv_local("accounts.csv")

    output_rows = []

    for row in pre_patch_rows:
        account_id = row["account_id"]
        role_name = row["role_name"]
        region = row["region"]

        logger.info(
            f"Processing account={account_id}, region={region}, role={role_name}"
        )
        #debug
        #print(f"account_id: {account_id}, role_name:{role_name}, region: {region}")

        #to assume the role
        session = assume_role(account_id, role_name, region)
        # Debug: confirm assumed account
        # sts = session.client("sts")
        # identity = sts.get_caller_identity()
        # print("Assumed identity:", identity)

        ssm = session.client("ssm")
        ec2 = session.client("ec2")
        s3  = session.client("s3")
        resourcegroups = session.client("resource-groups")
        response = ssm.describe_maintenance_windows()


        for mw in response["WindowIdentities"]:
            # if not mw.get("Enabled", True):
            #     continue
            logger.info(f"Checking MW {mw.get('Name')} (ID: {mw['WindowId']})")
            # Some MWs may not have future executions
            if "NextExecutionTime" not in mw:
                logger.debug(f"Skipping MW {mw.get('Name')} (no NextExecutionTime)")
                continue

            #Excluding MWs not starting with "mmpatching"
            if not mw.get("Name", "").startswith("mmpatching"):
                logger.debug(f"Skipping MW {mw.get('Name')} (MW not starting with 'mmpatching')")
                continue

            if runs_today(mw["NextExecutionTime"]):
                target_count = get_target_count(ssm,ec2,resourcegroups,mw["WindowId"])

                logger.info(
                    f"MW running today: {mw['Name']} "
                    f"(targets={target_count})"
                )

                output_rows.append({
                    "AccountId": account_id,
                    "Region": region,                    
                    "MaintenanceWindowId": mw["WindowId"],
                    "MaintenanceWindowName": mw["Name"],
                    "TargetInstanceCount": target_count
                })


    # Handle no data case Skip CSV + Email if no Maintenance Windows matched
    if not output_rows:
        logger.warning("No Maintenance Windows running today. Skipping notification.")
        print("No Maintenance Windows running today. Skipping notification.")
        return {
            "status": "success",
            "records_written": 0,
            "message": "No MWs found, notification skipped"
        }

    logger.info(
    "Summary of Maintenance Windows running today "
    "(Skipping MW not starting with 'mmpatching'):"
    )

    for idx, row in enumerate(output_rows, start=1):
        logger.info(
            "  %d) AccountId=%s | MWName=%s | MWId=%s | TargetCount=%s",
            idx,
            row.get("AccountId"),
            row.get("MaintenanceWindowName"),
            row.get("MaintenanceWindowId"),
            row.get("TargetInstanceCount")
        )

    # Write CSV to S3 (separate function)
    write_csv_to_s3(s3=s3,bucket=OUTPUT_BUCKET,key=OUTPUT_KEY,rows=output_rows)

    # Building html
    html_body = build_html_table(output_rows)

    #To send consolidated pre patch notifcation email, considering those account details separately in 
    #email_accounts.csv
    EMAIL_CONFIG_FILE = "email_account.csv"
    email_cfg = load_email_config(EMAIL_CONFIG_FILE)

    session = assume_role(
    account_id=email_cfg["account_id"],
    role_name=email_cfg["role_name"],
    region=email_cfg["region"]
    )

    send_email_ses(
    session=session,
    subject="Maintenance Windows Running Today",
    html_body=html_body,
    sender=EMAIL_FROM,
    recipients=EMAIL_TO,
    region=email_cfg["region"]
    )

    logger.info("Script completed successfully")

    return {
        "status": "success",
        "records_written": len(output_rows)
    }

if __name__ == "__main__":
    print("Running script locally")
    result = main()
    print(" Finished:", result)