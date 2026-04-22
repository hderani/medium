import argparse
from ado_epic_cloner.config import settings
from ado_epic_cloner.azure_client import get_connection
from ado_epic_cloner.services.epic_service import EpicService


def parse_args():
    parser = argparse.ArgumentParser(description="Clone Azure DevOps Epic")

    parser.add_argument("--main-epic-id", type=int, required=True)
    parser.add_argument("--user-mail", required=True)
    parser.add_argument("--user-name", required=True)
    parser.add_argument("--area-path", default="Area of Interest")
    parser.add_argument("--project", default="Area of Interest")
    parser.add_argument("--dry-run", action="store_true")

    return parser.parse_args()


def run(**kwargs):
    connection = get_connection(settings.azure_devops_url, settings.pat)

    service = EpicService(
        connection=connection,
        area_path=kwargs["area_path"],
        project=kwargs["project"],
        org_url=settings.azure_devops_url,
        dry_run=kwargs.get("dry_run", False),
    )

    epic_id = service.ensure_epic(
        kwargs["main_epic_id"],
        kwargs["user_name"],
        kwargs["user_mail"],
    )

    return service.clone_full_structure(
        kwargs["main_epic_id"],
        epic_id,
        kwargs["user_mail"],
    )


def main():
    args = parse_args()
    result = run(**vars(args))
    print(result)


if __name__ == "__main__":
    main()