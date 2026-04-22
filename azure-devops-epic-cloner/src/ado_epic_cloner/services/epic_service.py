from ado_epic_cloner.utils import work_items as wi


class EpicService:
    def __init__(self, connection, area_path, project, org_url, dry_run=False):
        self.connection = connection
        self.area_path = area_path
        self.project = project
        self.org_url = org_url
        self.dry_run = dry_run

    def ensure_epic(self, main_epic_id, user_name, user_mail):
        details = wi.get_work_item_details(self.connection, main_epic_id)
        title = f"{details['title']} - {user_name}"

        query = f"select * from WorkItems where [System.Title] = '{title}'"
        existing = wi.wiql_query(self.connection, query)

        if existing:
            print(f"Epic exists: {title}")
            return existing

        if self.dry_run:
            print(f"[DRY RUN] Would create Epic: {title}")
            return -1

        return wi.create_work_item(
            self.connection,
            details,
            None,
            user_mail,
            user_name,
            self.area_path,
            self.project,
            self.org_url,
        )

    def _clone_level(self, source_parent, target_parent, user_mail):
        mapping = {}
        children = wi.get_work_item_children(self.connection, source_parent)

        for child in children:
            rel, _ = wi.create_hierarchy_item(
                self.connection,
                child,
                target_parent,
                user_mail,
                self.area_path,
                self.project,
                self.org_url,
            )
            mapping.update(rel)

        return mapping

    def clone_full_structure(self, source_epic, target_epic, user_mail):
        features = self._clone_level(source_epic, target_epic, user_mail)

        user_stories = {}
        for old, new in features.items():
            user_stories.update(self._clone_level(old, new, user_mail))

        tasks = {}
        for old, new in user_stories.items():
            tasks.update(self._clone_level(old, new, user_mail))

        return {
            "features": features,
            "user_stories": user_stories,
            "tasks": tasks,
        }