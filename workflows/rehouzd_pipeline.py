# Upgrade Databricks SDK to the latest version and restart Python to see updated packages
%pip install --upgrade databricks-sdk==0.49.0
%restart_python

from databricks.sdk.service.jobs import JobSettings as Job


rehouizd_pipeline = Job.from_dict(
    {
        "name": "rehouizd_pipeline",
        "tasks": [
            {
                "task_key": "initialize_data_bnz_prps",
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/initialize_data_bnz_prps",
                    "base_parameters": {
                        "run_id": "{{job.run_id}}",
                        "current_date": "{{job.start_time.iso_date}}",
                    },
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
                "libraries": [
                    {
                        "pypi": {
                            "package": "faker",
                        },
                    },
                ],
            },
            {
                "task_key": "brnz_prps_prop_sales_dtl",
                "depends_on": [
                    {
                        "task_key": "initialize_data_bnz_prps",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/brnz_prps_prop_sales_dtl",
                    "base_parameters": {
                        "path_dst": "{{tasks.initialize_data_bnz_prps.values.path_dst}}",
                    },
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "slv_prps_prop_sales_dtl",
                "depends_on": [
                    {
                        "task_key": "brnz_prps_prop_sales_dtl",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/slv_prps_prop_sales_dtl",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "slv_int_inv_dtl",
                "depends_on": [
                    {
                        "task_key": "slv_prps_prop_sales_dtl",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/slvr_int_inv_dtl",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "process_slv_int_prp",
                "depends_on": [
                    {
                        "task_key": "slv_int_inv_dtl",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/process_slv_int_prp",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "process_slv_int_pr_cmps",
                "depends_on": [
                    {
                        "task_key": "process_slv_int_prp",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/process_slv_int_pr_cmps",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "prc_slv_int_prcl_sls_dlt_pl",
                "depends_on": [
                    {
                        "task_key": "process_slv_int_pr_cmps",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/prc_slv_int_prcl_sls_dlt_pl",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
            {
                "task_key": "prc_slv_int_prop_sls_dlt_pl",
                "depends_on": [
                    {
                        "task_key": "process_slv_int_pr_cmps",
                    },
                ],
                "notebook_task": {
                    "notebook_path": "/Workspace/Users/debrup.kar@outlook.com/rehouzd_code/prc_slv_int_prop_sls_dlt_pl",
                    "source": "WORKSPACE",
                },
                "job_cluster_key": "Job_cluster",
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "16.4.x-scala2.12",
                    "azure_attributes": {
                        "first_on_demand": 1,
                        "availability": "ON_DEMAND_AZURE",
                        "spot_bid_max_price": 100,
                    },
                    "node_type_id": "Standard_DS3_v2",
                    "enable_elastic_disk": True,
                    "policy_id": "001446205AB21A3D",
                    "data_security_mode": "USER_ISOLATION",
                    "runtime_engine": "STANDARD",
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8,
                    },
                },
            },
        ],
        "queue": {
            "enabled": True,
        },
    }
)

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
w.jobs.reset(new_settings=rehouizd_pipeline, job_id=587694243801070)
# or create a new job using: w.jobs.create(**rehouizd_pipeline.as_shallow_dict())
