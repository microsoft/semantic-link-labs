import sempy
import sempy.fabric as fabric
import pandas as pd
from typing import List, Optional, Union

def create_connection_cloud(name: str, server_name: str, database_name: str, user_name: str, password: str, privacy_level: str):

    #https://review.learn.microsoft.com/en-us/rest/api/fabric/core/connections/create-connection?branch=features%2Fdmts&tabs=HTTP

    df = pd.DataFrame(columns=['Connection ID', 'Connection Name', 'Connectivity Type', 'Connection Type', 'Connection Path', 'Privacy Level', 'Credential Type', 'Single Sign On Type', 'Connection Encryption', 'Skip Test Connection'])

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": "ShareableCloud",
        "name": name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
            {
                "name": "server",
                "value": server_name
            },
            {
                "name": "database",
                "value": database_name
            }
            ]
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False,
            "credentials": {
            "credentialType": "Basic",
            "username": user_name,
            "password": password
            }
        }
    }

    response = client.post(f"/v1/connections",json=request_body)

    if response.status_code == 200:
        o = response.json()
        new_data = {'Connection Id': o['id'], 'Connection Name': o['name'], 'Connectivity Type': o['connectivityType'],
                    'Connection Type': o['connectionDetails']['type'], 'Connection Path': o['connectionDetails']['path'], 'Privacy Level': o['privacyLevel'],
                    'Credential Type': o['credentialDetails']['credentialType'], 'Single Sign On Type': o['credentialDetails']['singleSignOnType'], 
                    'Connection Encryption': o['credentialDetails']['connectionEncryption'], 'Skip Test Connection': o['credentialDetails']['skipTestConnection']
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        df['Skip Test Connection'] = df['Skip Test Connection'].astype(bool)

        return df
    else:
        print(response.status_code)

def create_connection_on_prem(name: str, gateway_id: str, server_name: str, database_name: str, credentials: str, privacy_level: str):

    df = pd.DataFrame(columns=['Connection ID', 'Connection Name', 'Gateway ID', 'Connectivity Type', 'Connection Type', 'Connection Path', 'Privacy Level', 'Credential Type', 'Single Sign On Type', 'Connection Encryption', 'Skip Test Connection'])

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": "OnPremisesDataGateway",
        "gatewayId": gateway_id,
        "name": name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
            {
                "name": "server",
                "value": server_name
            },
            {
                "name": "database",
                "value": database_name
            }
            ]
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "NotEncrypted",
            "skipTestConnection": False,
            "credentials": {
            "credentialType": "Windows",
            "values": [
                {
                "gatewayId": gateway_id,
                "credentials": credentials
                }
            ]
            }
        }
    }

    response = client.post(f"/v1/connections",json=request_body)

    if response.status_code == 200:
        o = response.json()
        new_data = {'Connection Id': o['id'], 'Connection Name': o['name'], 'Gateway ID': o['gatewayId'], 'Connectivity Type': o['connectivityType'],
                    'Connection Type': o['connectionDetails']['type'], 'Connection Path': o['connectionDetails']['path'], 'Privacy Level': o['privacyLevel'],
                    'Credential Type': o['credentialDetails']['credentialType'], 'Single Sign On Type': o['credentialDetails']['singleSignOnType'], 
                    'Connection Encryption': o['credentialDetails']['connectionEncryption'], 'Skip Test Connection': o['credentialDetails']['skipTestConnection']
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        df['Skip Test Connection'] = df['Skip Test Connection'].astype(bool)

        return df
    else:
        print(response.status_code)

def create_connection_vnet(name: str, gateway_id: str, server_name: str, database_name: str, user_name: str, password: str, privacy_level: str):

    df = pd.DataFrame(columns=['Connection ID', 'Connection Name', 'Gateway ID', 'Connectivity Type', 'Connection Type', 'Connection Path', 'Privacy Level', 'Credential Type', 'Single Sign On Type', 'Connection Encryption', 'Skip Test Connection'])

    client = fabric.FabricRestClient()

    request_body = {
        "connectivityType": "VirtualNetworkDataGateway",
        "gatewayId": gateway_id,
        "name": name,
        "connectionDetails": {
            "type": "SQL",
            "parameters": [
            {
                "name": "server",
                "value": server_name
            },
            {
                "name": "database",
                "value": database_name
            }
            ]
        },
        "privacyLevel": privacy_level,
        "credentialDetails": {
            "singleSignOnType": "None",
            "connectionEncryption": "Encrypted",
            "skipTestConnection": False,
            "credentials": {
            "credentialType": "Basic",
            "username": user_name,
            "password": password
            }
        }
    }

    response = client.post(f"/v1/connections",json=request_body)

    if response.status_code == 200:
        o = response.json()
        new_data = {'Connection Id': o['id'], 'Connection Name': o['name'], 'Gateway ID': o['gatewayId'], 'Connectivity Type': o['connectivityType'],
                    'Connection Type': o['connectionDetails']['type'], 'Connection Path': o['connectionDetails']['path'], 'Privacy Level': o['privacyLevel'],
                    'Credential Type': o['credentialDetails']['credentialType'], 'Single Sign On Type': o['credentialDetails']['singleSignOnType'], 
                    'Connection Encryption': o['credentialDetails']['connectionEncryption'], 'Skip Test Connection': o['credentialDetails']['skipTestConnection']
        }
        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)

        df['Skip Test Connection'] = df['Skip Test Connection'].astype(bool)

        return df
    else:
        print(response.status_code)