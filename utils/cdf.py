import yaml, pathlib, os, re
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials, OAuthInteractive

def get_cognite_client(config_path=None, env: str = "dev"):
    if config_path is None:
        config_path = "local_config.yaml"

    with open(config_path, "r") as config_file:
        configs = yaml.safe_load(config_file)

    try:
        cognite_config = configs.get('cognite').get(env)

        if not cognite_config:
            raise Exception()
    except:
        raise Exception("Cognite credentials not configured for the environment.")

    match cognite_config['mode']:
        case "oauth":
            credentials = OAuthClientCredentials(
                token_url=f"https://login.microsoftonline.com/{cognite_config['tenant-id']}/oauth2/v2.0/token",
                client_id=cognite_config['client-id'],
                client_secret=cognite_config['client-secret'],
                scopes=[f"{cognite_config['base-url']}/.default"],
            )
        case "interactive_oauth":
            credentials = OAuthInteractive(
                authority_url=f"https://login.microsoftonline.com/{cognite_config['tenant-id']}",
                client_id=cognite_config['client-id'],
                scopes=[f"{cognite_config['base-url']}/.default"],
            )
        case _:
            raise Exception("Unknow auth mode.")

    cnf = ClientConfig(
        client_name=cognite_config["client-name"],
        base_url=cognite_config["base-url"],
        project=cognite_config["project"],
        credentials=credentials,
        debug=False,
    )

    return CogniteClient(cnf)