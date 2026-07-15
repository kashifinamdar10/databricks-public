# Databricks notebook source

import requests

import json

import os

import pprint

import time

import sys

 

try:

    primary_host = sys.argv[1]

    primary_token = sys.argv[2]

    dr_host = sys.argv[3]

    dr_token = sys.argv[4]

except IndexError:

    # Notebook mode: pull from widgets when sys.argv isn't populated
    dbutils.widgets.text("primary_host", "")

    dbutils.widgets.text("primary_token", "")

    dbutils.widgets.text("dr_host", "")

    dbutils.widgets.text("dr_token", "")

    primary_host = dbutils.widgets.get("primary_host")

    primary_token = dbutils.widgets.get("primary_token")

    dr_host = dbutils.widgets.get("dr_host")

    dr_token = dbutils.widgets.get("dr_token")

 

# COMMAND ----------

 

class add_users:

    def __init__(self,primary_host,primary_token,dr_host=None,dr_token= None,add_type='users',return_type=None):

        self.primary_host = primary_host

        self.dr_host = dr_host

        self.primary_token = primary_token

        self.dr_token = dr_token

        self.add_type = add_type

        self.return_type = return_type

 

    ##########

 

    def get_header(self,token):

        headers ={

            "Authorization": f"Bearer {token}",

            "Content-Type": "application/json"

        }

 

        return headers



    def _safe_json(self, response):
        # SCIM responses are not always JSON (empty body, gateway HTML on 5xx,
        # 429 throttle). Return {} instead of raising so one bad response never
        # kills the whole sync; keep the raw text available for logging.
        body = (response.text or "").strip()
        if not body:
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            print(f"non-JSON response status={response.status_code} body={response.text!r}")
            return {"_raw": response.text}

    def _scim_list_all(self, host, token, resource):
        # Page through a SCIM list endpoint (Users / ServicePrincipals / Groups)
        # so we return ALL resources, not just the first 100 (the SCIM default).
        headers = self.get_header(token)
        url = f"{host}/api/2.0/preview/scim/v2/{resource}"
        results = []
        start_index = 1
        page_size = 100
        while True:
            response = requests.get(url, headers=headers,
                                    params={"startIndex": start_index, "count": page_size})
            data = self._safe_json(response)
            page = data.get('Resources', [])
            results.extend(page)
            total = int(data.get('totalResults', 0))
            start_index += page_size
            if not page or start_index > total:
                break
        return results

    ##############add SPN function

    def list_add_spn(self):

        payloads = []

        r = []

        primary_host = self.primary_host

        primary_token= self.primary_token

        dr_host = self.dr_host

        dr_token = self.dr_token

        data = self._scim_list_all(primary_host, primary_token, "ServicePrincipals")

        for a in data:

            if 'displayName' in a:

                displayName = a['displayName']

                app_id = a['applicationId']

                json_add = {"displayName":displayName,"applicationId":app_id,"active":True}

                payloads.append(json_add)

            else:

                displayName = 'Unnamed service principal'

                app_id = a['applicationId']

                json_add = {"displayName":displayName,"applicationId":app_id,"active":True}

                payloads.append(json_add)

 

        for p in payloads:

            errors = []

            try:

                dr_url = f"{dr_host}/api/2.0/preview/scim/v2/ServicePrincipals"

                dr_headers = self.get_header(self.dr_token)

                response = requests.post(dr_url, headers = dr_headers,data = json.dumps(p))

                r.append(response.status_code)

            except Exception as e:

                print(str(e))

                errors.append(e)

 

        return errors,r

 

 

    def find_primary_users(self):

        payload = []

        if self.add_type == 'users':

            for u in self._scim_list_all(self.primary_host, self.primary_token, "Users"):

                user_email = u['emails']

                if 'displayName' in u and 'name' in u:

                    display_name = u['displayName']

                    name = u['name']

                else:

                    display_name = ''

                    name = {}

                active = u['active']

                user_name = u['userName']

                user_data = {'emails': user_email,

                    'displayName': display_name,

                    'name': name,

                    'active': active,

                    'userName': user_name

                }

 

                payload.append(user_data)

 

            return payload

        else:

            pass

 

    def create_users(self):

        dr_users = []

        count_of_users_added = 0

        payload = []

        error = []

        endpoint = self.dr_host

        token = self.dr_token

        headers = self.get_header(token)

        list_of_users_in_primary_workspace = self.find_primary_users

 

        def create_user(endpoint,headers, user_data):

            headers = self.get_header(token)

            response = requests.post(endpoint, headers=headers, json=user_data)

            # Log status + raw body so a rejected create is diagnosable instead
            # of crashing the whole run. SCIM does not always return JSON (empty
            # body, gateway HTML on 5xx, 429 throttle), which makes json.loads
            # raise "Expecting value: line 1 column 1 (char 0)".

            print(f"create_user {user_data.get('userName')} -> status={response.status_code} body={response.text!r}")

            try:

                data = response.json()

            except ValueError:

                data = {"_raw": response.text}

            return response.status_code, data

 

 

 

        #####################get the list of users in DR in list (paginated)

        api_url = f"{endpoint}/api/2.0/preview/scim/v2/Users"

        start_index = 1

        page_size = 100

        while True:

            paged_url = f"{api_url}?startIndex={start_index}&count={page_size}"

            response = requests.get(paged_url, headers = headers)

            try:

                data = response.json()

            except ValueError:

                print(f"list DR users -> status={response.status_code} body={response.text!r}")

                break

            resources = data.get('Resources', [])

            for a in resources:

                if 'userName' in a:

                    dr_users.append(a['userName'])

            total = int(data.get('totalResults', 0))

            start_index += page_size

            if not resources or start_index > total:

                break

 

 

        if self.add_type == 'users':

            list_of_primary_users = self.find_primary_users()

 

            for user in list_of_primary_users:

                if user['userName'] in dr_users:

                    print(f"{user['userName']} already created")

 

                else:

                    time.sleep(2)

                    status, text = create_user(api_url, token,user)

 

                    if status == 201:

                        print(f"{user['displayName']} created successfully")

                        count_of_users_added += 1

                    else:

                        error.append(user)

                        print(f"{user['displayName']} failed with code {status} and text is {text}")

 

        else:

            pass

 

 

        return count_of_users_added, error


    ##############add group sync function
    def list_add_groups(self):
        errors = []
        statuses = []
        primary_host = self.primary_host
        primary_token = self.primary_token
        dr_host = self.dr_host
        dr_token = self.dr_token
        dr_headers = self.get_header(dr_token)

        # primary id -> natural key (userName / applicationId / displayName)
        primary_users = self._scim_list_all(primary_host, primary_token, "Users")
        primary_user_id_to_name = {u['id']: u['userName'] for u in primary_users if 'id' in u and 'userName' in u}

        primary_spns = self._scim_list_all(primary_host, primary_token, "ServicePrincipals")
        primary_spn_id_to_app = {sp['id']: sp['applicationId'] for sp in primary_spns if 'id' in sp and 'applicationId' in sp}

        primary_groups = self._scim_list_all(primary_host, primary_token, "Groups")
        primary_group_id_to_name = {g['id']: g['displayName'] for g in primary_groups if 'id' in g and 'displayName' in g}

        # DR natural key -> DR id (so primary member ids can be translated)
        dr_group_url = f"{dr_host}/api/2.0/preview/scim/v2/Groups"
        dr_users = self._scim_list_all(dr_host, dr_token, "Users")
        dr_name_to_user_id = {u['userName']: u['id'] for u in dr_users if 'id' in u and 'userName' in u}

        dr_spns = self._scim_list_all(dr_host, dr_token, "ServicePrincipals")
        dr_app_to_spn_id = {sp['applicationId']: sp['id'] for sp in dr_spns if 'id' in sp and 'applicationId' in sp}

        dr_groups = self._scim_list_all(dr_host, dr_token, "Groups")
        dr_name_to_group_id = {g['displayName']: g['id'] for g in dr_groups if 'id' in g and 'displayName' in g}

        # pass 1: create groups (without members) for displayNames missing in DR.
        # creating empty first means pass 2 can resolve nested-group member refs.
        for g in primary_groups:
            display_name = g.get('displayName')
            if not display_name:
                continue
            if display_name in dr_name_to_group_id:
                print(f"{display_name} group already created")
                continue
            try:
                payload = {"displayName": display_name}
                response = requests.post(dr_group_url, headers=dr_headers, data=json.dumps(payload))
                statuses.append(response.status_code)
                if response.status_code == 201:
                    created = self._safe_json(response)
                    dr_name_to_group_id[display_name] = created.get('id')
                    print(f"{display_name} group created successfully")
                else:
                    errors.append({"group": display_name, "code": response.status_code, "body": response.text})
                    print(f"{display_name} group failed with code {response.status_code} and text is {response.text}")
                time.sleep(2)
            except Exception as e:
                print(str(e))
                errors.append({"group": display_name, "error": str(e)})

        # pass 2: PATCH members onto each DR group, translating ids via natural keys
        for g in primary_groups:
            display_name = g.get('displayName')
            if not display_name:
                continue
            target_group_id = dr_name_to_group_id.get(display_name)
            if not target_group_id:
                continue
            resolved_members = []
            for m in g.get('members', []) or []:
                ref = m.get('$ref', '')
                value = m.get('value')
                if not value:
                    continue
                if ref.startswith('Users/') or value in primary_user_id_to_name:
                    dr_id = dr_name_to_user_id.get(primary_user_id_to_name.get(value))
                elif ref.startswith('ServicePrincipals/') or value in primary_spn_id_to_app:
                    dr_id = dr_app_to_spn_id.get(primary_spn_id_to_app.get(value))
                elif ref.startswith('Groups/') or value in primary_group_id_to_name:
                    dr_id = dr_name_to_group_id.get(primary_group_id_to_name.get(value))
                else:
                    dr_id = None
                if dr_id:
                    resolved_members.append({"value": dr_id})

            if not resolved_members:
                continue

            try:
                patch_url = f"{dr_group_url}/{target_group_id}"
                patch_payload = {
                    "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                    "Operations": [{"op": "add", "path": "members", "value": resolved_members}]
                }
                response = requests.patch(patch_url, headers=dr_headers, data=json.dumps(patch_payload))
                statuses.append(response.status_code)
                if response.status_code in (200, 204):
                    print(f"{display_name} group members synced")
                else:
                    errors.append({"group": display_name, "patch_code": response.status_code, "body": response.text})
                    print(f"{display_name} group member sync failed with code {response.status_code} and text is {response.text}")
                time.sleep(2)
            except Exception as e:
                print(str(e))
                errors.append({"group": display_name, "error": str(e)})

        return errors, statuses


# COMMAND ----------

 

a = add_users(primary_host,primary_token,dr_host,dr_token)

status,error = a.create_users()

status,error = a.list_add_spn()

status,error = a.list_add_groups()