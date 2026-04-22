from typing import Dict, Any, List, Optional
from azure.devops.v7_1.work_item_tracking import JsonPatchOperation, Wiql


# -------------------------------
# Helpers
# -------------------------------

def _safe_get(fields: Dict[str, Any], key: str, default=None):
    return fields.get(key, default)


def _create_patch_operation(op: str, path: str, value: Any) -> JsonPatchOperation:
    patch = JsonPatchOperation()
    patch.op = op
    patch.path = path
    patch.value = value
    return patch


def _field_op(field: str, value: Any) -> JsonPatchOperation:
    return _create_patch_operation('add', f'/fields/{field}', value)


# -------------------------------
# Core API wrappers
# -------------------------------

def get_work_item_details(connection, wi_id: int) -> Dict[str, Any]:
    client = connection.clients.get_work_item_tracking_client()
    work_item = client.get_work_item(wi_id, expand="all")
    fields = work_item.fields

    return {
        "type": _safe_get(fields, 'System.WorkItemType'),
        "title": _safe_get(fields, 'System.Title'),
        "description": _safe_get(fields, 'System.Description'),
        "criteria": _safe_get(fields, 'Microsoft.VSTS.Common.AcceptanceCriteria'),
        "area": _safe_get(fields, 'System.AreaPath'),
        "iteration": _safe_get(fields, 'System.IterationPath'),
        "assigned": (_safe_get(fields, 'System.AssignedTo') or {}).get('displayName'),
        "order_in_epic": _safe_get(fields, 'Custom.SequenceInEpic'),
        "order_in_feat": _safe_get(fields, 'Custom.SequenceInFeature'),
        "parent": _safe_get(fields, 'System.Parent'),
        "tags": _safe_get(fields, 'System.Tags'),
    }


def wiql_query(connection, query: str, return_field: str = 'id') -> Optional[Any]:
    client = connection.clients.get_work_item_tracking_client()
    wiql = Wiql(query=query)

    results = client.query_by_wiql(wiql, top=1).work_items
    if not results:
        return None

    res = results[0]
    return res.id if return_field == 'id' else res.url


def get_work_item_children(connection, wi_id: int) -> List[int]:
    client = connection.clients.get_work_item_tracking_client()
    work_item = client.get_work_item(wi_id, expand="all")

    children = []
    for rel in work_item.relations or []:
        if rel.attributes.get('name') == 'Child':
            children.append(int(rel.url.split('/')[-1]))

    return children


# -------------------------------
# Work item creation
# -------------------------------

def build_patch_document(
    details: Dict[str, Any],
    user: str,
    user_name: Optional[str],
    area_path: str,
    parent_id: Optional[int],
    org_url: str,
    project: str
) -> List[JsonPatchOperation]:

    patch = []

    # Required fields
    patch.append(_field_op('System.AreaPath', area_path))
    patch.append(_field_op('System.AssignedTo', user))

    # Title
    title = f"{details['title']} - {user_name}" if user_name else details['title']
    patch.append(_field_op('System.Title', title))

    # Optional fields
    optional_fields = {
        'System.Description': details.get('description'),
        'System.Tags': details.get('tags'),
        'Microsoft.VSTS.Common.AcceptanceCriteria': details.get('criteria')
    }

    for field, value in optional_fields.items():
        if value:
            patch.append(_field_op(field, value))

    # Parent relation
    if parent_id:
        parent_url = f"{org_url}/{project}/_apis/wit/workItems/{parent_id}"
        patch.append(_create_patch_operation(
            'add',
            '/relations/-',
            {'rel': 'System.LinkTypes.Hierarchy-Reverse', 'url': parent_url}
        ))

    # Custom ordering
    if details['type'] == 'Feature' and details.get('order_in_epic'):
        patch.append(_field_op('Custom.SequenceInEpic', details['order_in_epic']))

    if details['type'] == 'User Story' and details.get('order_in_feat'):
        patch.append(_field_op('Custom.SequenceInFeature', details['order_in_feat']))

    return patch


def create_work_item(
    connection,
    details: Dict[str, Any],
    parent_id: Optional[int],
    user: str,
    user_name: Optional[str],
    area_path: str,
    project: str,
    org_url: str
) -> int:

    client = connection.clients.get_work_item_tracking_client()

    patch = build_patch_document(
        details, user, user_name, area_path, parent_id, org_url, project
    )

    created = client.create_work_item(
        document=patch,
        project=project,
        type=details['type']
    )

    print(f"{details['type']} created: {created.id}")
    return created.id


# -------------------------------
# Query helpers
# -------------------------------

def find_existing_item(connection, details, user, area_path):
    query = f"""
    select * from WorkItems
    where [System.Title] = '{details['title']}'
    and [System.AssignedTo] = '{user}'
    and [System.WorkItemType] = '{details['type']}'
    and [System.AreaPath] = '{area_path}'
    """
    return wiql_query(connection, query)


# -------------------------------
# Hierarchy logic
# -------------------------------

def create_hierarchy_item(
    connection,
    source_id: int,
    target_parent_id: int,
    user: str,
    area_path: str,
    project: str,
    org_url: str
):
    mapping = {}
    created_flag = 0

    details = get_work_item_details(connection, source_id)
    existing_id = find_existing_item(connection, details, user, area_path)

    if existing_id:
        print(f"{details['type']} exists: {details['title']}")
        mapping[source_id] = existing_id
    else:
        new_id = create_work_item(
            connection,
            details,
            target_parent_id,
            user,
            None,
            area_path,
            project,
            org_url
        )
        mapping[source_id] = new_id
        created_flag = 1

    return mapping, created_flag