from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


# =========================================================
# CONFIGURATION
# =========================================================

PREDICTOPS_DIR = "/opt/airflow/predictops"


# =========================================================
# DAG
# =========================================================

with DAG(
    dag_id="predictops_training_pipeline",

    description="PredictOps automated ML training and evaluation pipeline",

    start_date=datetime(2026, 1, 1),

    schedule="0 2 * * *",

    catchup=False,

    tags=[
        "PredictOps",
        "Machine Learning",
        "Predictive Maintenance",
        "MLOps",
    ],
) as dag:

    # -----------------------------------------------------
    # 1. Validate environment and dataset
    # -----------------------------------------------------

    validate_data = BashOperator(
        task_id="validate_data",

        bash_command=f"""
        echo "=========================================="
        echo "PREDICTOPS DATA VALIDATION"
        echo "=========================================="

        cd {PREDICTOPS_DIR}

        echo ""
        echo "Checking dataset..."

        test -f data/processed/train_FD001_processed.csv

        echo "Dataset found."

        echo ""
        echo "Checking training script..."

        test -f src/models/train.py

        echo "Training script found."

        echo ""
        echo "Checking MLflow..."

        python -c "import mlflow; print('MLflow:', mlflow.__version__)"

        echo ""
        echo "Validation completed successfully."
        """,
    )


    # -----------------------------------------------------
    # 2. Train model
    # -----------------------------------------------------

    train_model = BashOperator(
        task_id="train_model",

        bash_command=f"""
        echo "=========================================="
        echo "PREDICTOPS MODEL TRAINING"
        echo "=========================================="

        cd {PREDICTOPS_DIR}

        python src/models/train.py
        """,
    )


    # -----------------------------------------------------
    # 3. Evaluate model
    # -----------------------------------------------------

    evaluate_model = BashOperator(
        task_id="evaluate_model",

        bash_command=f"""
        echo "=========================================="
        echo "PREDICTOPS MODEL EVALUATION"
        echo "=========================================="

        cd {PREDICTOPS_DIR}

        python src/models/evaluate.py
        """,
    )


    # -----------------------------------------------------
    # PIPELINE ORDER
    # -----------------------------------------------------

    validate_data >> train_model >> evaluate_model