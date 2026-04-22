# ADO Epic Cloner

Clone an Azure DevOps Epic hierarchy (Features → User Stories → Tasks)

## Usage

```bash
ado-clone-epic \
  --main-epic-id 123 \
  --user-mail user@company.com \
  --user-name "Jane Doe"
```

## Folder Structure
```
ado-epic-cloner/
│
├── src/
│   └── ado_epic_cloner/
│       ├── __init__.py
│
│       ├── main.py
│       ├── config.py
│       ├── azure_client.py
│
│       ├── services/
│       │   └── epic_service.py
│
│       ├── utils/
│       │   └── work_items.py
│
├── pipelines/
│   └── clone-epic.yml
│
├── azure-pipelines.yml
├── requirements.txt
├── .env.example
├── README.md
└── pyproject.toml
```