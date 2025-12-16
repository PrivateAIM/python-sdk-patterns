class MockNodeConfig:
    def __init__(self, test_kwargs) -> None:
        # init analysis status
        self.finished = False
        # environment variables
        self.analysis_id = "uuid"
        self.project_id = test_kwargs.get('project_id','project_uuid')
        # tbd by MessageBroker
        self.node_role = test_kwargs.get('role','default')
        self.node_id = test_kwargs.get('node_id','node_uuid')
