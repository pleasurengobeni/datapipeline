import sys
import os
from pathlib import Path

# Add project root to sys.path so we can import modules
current_file = Path(__file__).resolve()
# dags/modules/test.py -> dags/modules -> dags -> root
project_root = current_file.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Import naming module (local or via full path)
try:
    import naming
except ImportError:
    from dags.modules import naming

def test_naming_consistency():
    print("========================================")
    print("   Testing Naming Module Consistency    ")
    print("========================================\n")
    
    # Mock base directories
    config_dir = Path("/opt/airflow/dags/config")
    etl_dir = Path("/opt/airflow/dags/etl")

    # ---------------------------------------------------------
    # Scenario 1: Spark File-Based Pipeline
    # ---------------------------------------------------------
    print("--- Scenario 1: Spark File Job (Input: 'Wine Quality') ---")
    bare_name_1 = "Wine Quality"
    config_1 = {
        "pipeline_type": "file_based",
        "spark": {"enabled": True},
        "target": {
            "dev": {
                "target_db_conn_id": "Cenfri_Prd_Con",
            }
        }
    }
    
    # 1. Generate ID
    dag_id_1 = naming.generate_dag_id(config_1, bare_name_1)
    print(f"Generated DAG ID:  {dag_id_1}")

    # 2. Get Config Path
    cfg_path_1 = naming.get_config_path(dag_id_1, config_1, config_dir)
    print(f"Config JSON Path:  {cfg_path_1}")

    # 3. Get Python DAG Path
    py_path_1 = naming.get_dag_py_path(dag_id_1, config_1, etl_dir)
    print(f"Python DAG Path:   {py_path_1}")

    # Assertions
    expected_id_1 = "spark_file_cenfri_prd_con_wine_quality"
    assert dag_id_1 == expected_id_1, f"Expected {expected_id_1}, got {dag_id_1}"
    assert cfg_path_1.name == f"{expected_id_1}.json", "Config filename mismatch"
    assert py_path_1.name == f"{expected_id_1}.py", "Python filename mismatch"
    assert cfg_path_1.parent.name == "cenfri_prd_con", "Config folder mismatch"
    print("✅ Scenario 1 Passed: Spark File Job names are consistent.\n")


    # ---------------------------------------------------------
    # Scenario 2: Standard DB-to-DB Pipeline (No Spark)
    # ---------------------------------------------------------
    print("--- Scenario 2: DB Job (Input: 'Invoices Daily') ---")
    bare_name_2 = "Invoices Daily"
    config_2 = {
        "pipeline_type": "db",
        # No spark key implies pandas/standard db
        "target": {
            "dev": {
                "target_db_conn_id": "Finance-DB",
            }
        }
    }

    # 1. Generate ID
    dag_id_2 = naming.generate_dag_id(config_2, bare_name_2)
    print(f"Generated DAG ID:  {dag_id_2}")

    # 2. Get Config Path
    cfg_path_2 = naming.get_config_path(dag_id_2, config_2, config_dir)
    print(f"Config JSON Path:  {cfg_path_2}")

    # 3. Get Python DAG Path
    py_path_2 = naming.get_dag_py_path(dag_id_2, config_2, etl_dir)
    print(f"Python DAG Path:   {py_path_2}")

    # Assertions
    expected_id_2 = "db_finance_db_invoices_daily"
    assert dag_id_2 == expected_id_2, f"Expected {expected_id_2}, got {dag_id_2}"
    assert cfg_path_2.name == f"{expected_id_2}.json", "Config filename mismatch"
    assert py_path_2.name == f"{expected_id_2}.py", "Python filename mismatch"
    print("✅ Scenario 2 Passed: DB Job names are consistent.\n")

    # ---------------------------------------------------------
    # Scenario 3: API Pipeline (pipeline_type "hybrid" + source_type "api" —
    # a standalone job type from the user's perspective, implemented by
    # reusing hybrid's raw -> refined -> target code)
    # ---------------------------------------------------------
    print("--- Scenario 3: API Job (Input: 'Weather Feed') ---")
    bare_name_3 = "Weather Feed"
    config_3 = {
        "pipeline_type": "hybrid",
        "source_type": "api",
        "target": {
            "dev": {
                "target_db_conn_id": "Analytics_DW",
            }
        }
    }

    # 1. Generate ID
    dag_id_3 = naming.generate_dag_id(config_3, bare_name_3)
    print(f"Generated DAG ID:  {dag_id_3}")

    # 2. Get Config Path
    cfg_path_3 = naming.get_config_path(dag_id_3, config_3, config_dir)
    print(f"Config JSON Path:  {cfg_path_3}")

    # 3. Get Python DAG Path
    py_path_3 = naming.get_dag_py_path(dag_id_3, config_3, etl_dir)
    print(f"Python DAG Path:   {py_path_3}")

    # Assertions
    expected_id_3 = "api_analytics_dw_weather_feed"
    assert dag_id_3 == expected_id_3, f"Expected {expected_id_3}, got {dag_id_3}"
    assert cfg_path_3.name == f"{expected_id_3}.json", "Config filename mismatch"
    assert py_path_3.name == f"{expected_id_3}.py", "Python filename mismatch"
    print("✅ Scenario 3 Passed: API Job names are consistent.\n")

    # ---------------------------------------------------------
    # Scenario 4: Real file/db Hybrid Pipeline — must still get the plain
    # "hybrid" prefix now that source_type "api" shares pipeline_type "hybrid"
    # ---------------------------------------------------------
    print("--- Scenario 4: Hybrid Job, non-API source (Input: 'New Connections') ---")
    bare_name_4 = "New Connections"
    config_4 = {
        "pipeline_type": "hybrid",
        "source_type": "db",
        "target": {
            "dev": {
                "target_db_conn_id": "Cenfri_Dev_Con",
            }
        }
    }

    dag_id_4 = naming.generate_dag_id(config_4, bare_name_4)
    print(f"Generated DAG ID:  {dag_id_4}")

    expected_id_4 = "hybrid_cenfri_dev_con_new_connections"
    assert dag_id_4 == expected_id_4, f"Expected {expected_id_4}, got {dag_id_4}"
    print("✅ Scenario 4 Passed: Non-API Hybrid Job names are unaffected.\n")

if __name__ == "__main__":
    test_naming_consistency()