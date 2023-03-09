import os
from elasticsearch import Elasticsearch

print("Shipping to Elasticsearch")

es_url = os.environ.get('ES_HOST')
es_username = os.environ.get('ES_USERNAME')
es_password = os.environ.get('ES_PASSWORD')
es = Elasticsearch(es_url, http_auth=(es_username, es_password))

index = "github_pr_details"

body = {
    "mappings": {
        "properties": {
            "repository_name": {"type": "keyword"},
            "pr_number": {"type": "integer"},
            "pr_title": {"type": "text"},
            "pr_body": {"type": "text"},
            "pr_base_branch": {"type": "keyword"},
            "pr_head_branch": {"type": "keyword"},
            "pr_state": {"type": "keyword"},
            "pr_assignee": {"type": "keyword"},
            "pr_requested_reviewers": {"type": "keyword"},
            "pr_requested_teams": {"type": "keyword"},
            "pr_labels": {"type": "keyword"},
            "pr_url": {"type": "keyword"},
            "pr_created_at": {"type": "date"},
            "pr_updated_at": {"type": "date"},
            "pr_merged_at": {"type": "date"},
            "pr_closed_at": {"type": "date"}
        }
    },
    "settings": {
        "index": {
            "number_of_shards": 3,
            "number_of_replicas": 0
        }
    }
}


def get_assignes(assignees):
    '''This function returns a list of assignees'''
    if assignees != "":
        assigness_list = []
        for assignee in assignees:
            assigness_list.append(assignee['login'])
        return assigness_list
    else:
        return assignees


def get_requested_reviewers(requested_reviewers):
    '''This function returns a list of requested reviewers'''
    if requested_reviewers != "":
        requested_reviewers_list = []
        for requested_reviewer in requested_reviewers:
            requested_reviewers_list.append(requested_reviewer['login'])
        return requested_reviewers_list
    else:
        return requested_reviewers


def get_requested_teams(requested_teams):
    '''This function returns a list of requested teams'''
    if requested_teams != "":
        requested_teams_list = []
        for requested_team in requested_teams:
            requested_teams_list.append(requested_team['name'])
        return requested_teams_list
    else:
        return requested_teams


def get_labels(labels):
    '''This function returns a list of labels'''
    if labels != "":
        labels_list = []
        for label in labels:
            labels_list.append(label['name'])
        return labels_list
    else:
        return labels


def check_null(value):
    '''This function checks if a value is null'''
    if value == "" or value == None:
        value = "None"
        return value
    else:
        return value


def list_to_string(value):
    '''This function converts a list to a string'''
    value = ", ".join(value)
    return value


def es_reachable():
    '''This function checks if Elasticsearch is reachable'''
    return es.ping()


def index_exists():
    '''This function checks if an index exists'''
    return es.indices.exists(index=index)


def shipping_to_es(data):
    '''This function sends the data to Elasticsearch'''
    if not es_reachable:
        print("Elasticsearch is not reachable")
        return False
    if not index_exists():
        print("Index does not exist. Creating an index...")
        respose = es.indices.create(index=index, body=body)
        if not respose["acknowledged"]:
            print("Index creation failed")
            return False
    print("Shipping data to Elasticsearch")
    es.bulk(index=index, body=data)
    return True


def lambda_handler(event, context):
    '''This function is the handler for the Lambda function'''
    event = event['detail']
    try:
        bulk_api_body = []
        action = {
            "index": {
                "_index": index,
                "_id": event['pull_request']['id']
            }
        }
        eachpr = {
            "repository_name": event['repository']['name'],
            "pr_number": event['number'],
            "pr_title": check_null(event['pull_request']['title']),
            "pr_body": check_null(event['pull_request']['body']),
            "pr_base_branch": event['pull_request']['base']['ref'],
            "pr_head_branch": event['pull_request']['head']['ref'],
            "pr_state": event['pull_request']['state'],
            "pr_assignee": check_null(list_to_string(get_assignes(event['pull_request']['assignees']))),
            "pr_requested_reviewers": check_null(list_to_string(get_requested_reviewers(event['pull_request']['requested_reviewers']))),
            "pr_requested_teams": check_null(list_to_string(get_requested_teams(event['pull_request']['requested_teams']))),
            "pr_labels": check_null(list_to_string(get_labels(event['pull_request']['labels']))),
            "pr_url": event['pull_request']['html_url'],
            "pr_created_at": event['pull_request']['created_at'],
            "pr_updated_at": check_null(event['pull_request']['updated_at']),
            "pr_merged_at": check_null(event['pull_request']['merged_at']),
            "pr_closed_at": check_null(event['pull_request']['closed_at'])
        }
        bulk_api_body.append(action)
        bulk_api_body.append(eachpr)
        print(bulk_api_body)
        response = shipping_to_es(bulk_api_body)
        if not response:
            print("Failed to ship data to Elasticsearch")
            return "Shipping failed"
        return "Data shipped to Elasticsearch"
    except Exception as e:
        print(f"The Exception Occurred: {e}")
        raise e
