import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    def __init__(self):
        self.azure_devops_url = os.getenv("AZURE_DEVOPS_URL")
        self.pat = os.getenv("AZURE_DEVOPS_EXT_PAT")

        if not self.azure_devops_url:
            raise ValueError("Missing AZURE_DEVOPS_URL")
        if not self.pat:
            raise ValueError("Missing AZURE_DEVOPS_EXT_PAT")


settings = Settings()