import os
import boto3
import toml
import logging
import watchtower

def parse_envs():
    """
    Parse the environment variable `RUN_ENV`, set in the Dockerfile,
    to return a dictionary of strings that can be used by :class:`SecretsManager`
    :raises TypeError: if the `RUN_ENV` environment variable is not set.
        Valid options for `RUN_ENV` should be 'development', 'stage', or 'production'
    :returns: Dictionary of strings pertaining to credentials held in AWS Secret Manager.
    """

    run_environment = os.getenv("RUN_ENV")

    if run_environment:
        with open("config.toml", "r") as tml:
            cfg = toml.load(tml)
        return run_environment, cfg[run_environment]
    else:
        raise TypeError(
            "RUN_ENV environment variable is None. Set either 'development', 'stage', or 'production'"
        )

def create_logger(logger_name: str, log_group_name: str) -> logging.Logger:
    """return CloudWatch enabled logger"""
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    logging.basicConfig(level=logging.INFO)
    cloud_logger = logging.getLogger(logger_name)
    cloud_logger.addHandler(watchtower.CloudWatchLogHandler(log_group=log_group_name))

    return cloud_logger


def assume_role():
    env_name, environment_secrets = parse_envs()
    role_arn = environment_secrets["ROLE_ARN"]

    sts_client = boto3.client('sts')
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName="AssumeRoleSession1"
    )
    new_session = boto3.Session(aws_access_key_id=assumed_role_object['Credentials']['AccessKeyId'],
                                aws_secret_access_key=assumed_role_object['Credentials']['SecretAccessKey'],
                                aws_session_token=assumed_role_object['Credentials']['SessionToken'])
    return new_session

